import { v4 as uuid } from 'uuid'

// NOTE: Only non-primitive task types can be made readonly.
//   For example `Readonly<string>` results in `Readonly<{ toString: …, …}>` and TS will not even allow
//   the processTask function to be used to infer the string task type from `(task: string) => {}`,
//   requiring the `Readonly<string>` type instead.
type ActualTask<Task> = Task extends object ? Readonly<Task> : Task

interface QueueItem<Task, TaskResult> {
  task: Task
  key: string
  delayPromise: Promise<void> | null
  resolve: (result: TaskResult) => void
  reject: (error: any) => void
}

interface QueueOptions<Task> {
  key: (task: Task) => string
  merge: (existingTask: Task, incomingTask: Task) => Task
  precheck: (task: Task) => Promise<void> | void
  onDrain: () => void
  retryOnFailure: boolean
  retryBeforeOtherTasks: boolean
  retryDelay: number
  retryPrecheckOnFailure: boolean
  precheckRetryDelay: number
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(() => resolve(), ms))

export class Suprqueue<Task, TaskResult> {
  _processTask: (task: ActualTask<Task>) => Promise<TaskResult> | TaskResult
  _options: QueueOptions<ActualTask<Task>>

  _queue: Array<QueueItem<ActualTask<Task>, TaskResult>> = []
  _running: boolean = false
  _paused: boolean = false

  constructor(
    // WARN: TS cannot constrain the task type to be readonly unless it is specified as a type parameter of the class
    //   such as `new Suprqueue<MyTask, MyResult>((task) => {})`. Inferring the type from the `processTask` function
    //   as `new Suprqueue((task: MyTask) => {})` will allow mutation of the task object by the function
    //   (although other functions in the `options` object will work fine). It is also possible to set the readonly
    //   flag in the `processTask` function parameter as `(task: Readonly<MyTask>) => {}`.
    processTask: (task: ActualTask<Task>) => Promise<TaskResult> | TaskResult,
    options: Partial<QueueOptions<ActualTask<Task>>> = {}
  ) {
    this._processTask = processTask
    this._options = {
      key: options.key ?? (() => uuid()),
      merge: options.merge ?? ((_existingTask, incomingTask) => incomingTask),
      precheck: options.precheck ?? (() => {}),
      onDrain: options.onDrain ?? (() => {}),
      retryOnFailure: Boolean(options.retryOnFailure),
      retryBeforeOtherTasks: Boolean(options.retryBeforeOtherTasks),
      retryDelay: options.retryDelay ?? 0,
      retryPrecheckOnFailure: Boolean(options.retryPrecheckOnFailure),
      precheckRetryDelay: options.precheckRetryDelay ?? 0,
    }
  }

  pause() {
    this._paused = true
  }

  resume() {
    this._paused = false
    void this._processNextTask()
  }

  async pushTask(incomingTask: ActualTask<Task>): Promise<TaskResult> {
    return new Promise((resolve, reject) => {
      const key = this._options.key(incomingTask)
      const incomingItem = { task: incomingTask, key, delayPromise: sleep(0), resolve, reject }

      const existingKeyItemIndex = this._queue.findIndex((item) => item.key === key)
      const existingKeyItem = existingKeyItemIndex > -1 ? this._queue[existingKeyItemIndex] : null

      if (existingKeyItemIndex > -1 && existingKeyItem) {
        const mergedItem = this._mergeQueueItems(existingKeyItem, incomingItem)
        this._queue.splice(existingKeyItemIndex, 1, mergedItem)
      } else {
        this._queue.push(incomingItem)
      }

      void this._processQueue()
    })
  }

  private async _processQueue() {
    if (this._running) {
      return
    }
    if (this._paused) {
      return
    }

    await this._processNextTask()
  }

  private async _processNextTask(): Promise<void> {
    this._running = true
    // NOTE: We want to run the tasks asynchronusly, with a tiny delay, to allow synchronous
    //   queuing (and possibly merging) of multiple tasks.
    await sleep(0)
    if (this._paused) {
      return
    }

    // WARN: We must keep the item in the queue during preparation for its run.
    const currentItem = this._queue[0]
    if (typeof currentItem === 'undefined') {
      this._running = false
      this._options.onDrain()
      return
    }

    try {
      await currentItem.delayPromise
      if (this._paused) {
        return
      }

      try {
        await this._runPrecheckForTask(currentItem.task)
      } catch (err) {
        // WARN: We must remove the item from the queue if the precheck fails to avoid a retry loop of the task.
        this._queue.shift()
        throw err
      }
      if (this._paused) {
        return
      }

      if (currentItem !== this._queue[0]) {
        // NOTE: The item was modified (likely merged with an incoming task) while waiting for the retry delay
        //   or running the precheck. We are starting over, running the precheck again for the new task.
        return this._processNextTask()
      }

      // NOTE: Actual processing of the task starts here. We can now safely remove the task from the queue.
      this._queue.shift()

      try {
        const result = await this._processTask.call(null, currentItem.task)
        currentItem.resolve.call(null, result)
      } catch (taskErr) {
        if (!this._options.retryOnFailure) {
          throw taskErr
        }

        this._queueItemAsRetried(currentItem)
      }
    } catch (err) {
      currentItem.reject.call(null, err)
    }

    await this._processNextTask()
  }

  private _queueItemAsRetried(currentItem: QueueItem<ActualTask<Task>, TaskResult>) {
    const retriedItem = { ...currentItem, delayPromise: sleep(this._options.retryDelay) }

    if (this._options.retryBeforeOtherTasks) {
      const queuedKeyItemIndex = this._queue.findIndex((queuedItem) => queuedItem.key === currentItem.key)
      const queuedKeyItem = queuedKeyItemIndex > -1 ? this._queue[queuedKeyItemIndex] : null
      if (queuedKeyItemIndex > -1 && queuedKeyItem) {
        const mergedItem = this._mergeQueueItems(retriedItem, queuedKeyItem)
        this._queue.splice(queuedKeyItemIndex, 1)
        this._queue.unshift(mergedItem)
      } else {
        this._queue.unshift(retriedItem)
      }
    } else {
      const queuedKeyItemIndex = this._queue.findLastIndex((queuedItem) => queuedItem.key === currentItem.key)
      const queuedKeyItem = queuedKeyItemIndex > -1 ? this._queue[queuedKeyItemIndex] : null
      if (queuedKeyItemIndex > -1 && queuedKeyItem) {
        const mergedItem = this._mergeQueueItems(queuedKeyItem, retriedItem)
        this._queue.splice(queuedKeyItemIndex, 1, mergedItem)
      } else {
        this._queue.push(retriedItem)
      }
    }
  }

  private _mergeQueueItems(
    existingItem: QueueItem<ActualTask<Task>, TaskResult>,
    incomingItem: QueueItem<ActualTask<Task>, TaskResult>
  ): QueueItem<ActualTask<Task>, TaskResult> {
    const mergedTask = incomingItem ? this._options.merge(existingItem.task, incomingItem.task) : existingItem.task

    return {
      task: mergedTask,
      key: existingItem.key,
      delayPromise: Promise.all([existingItem.delayPromise, incomingItem.delayPromise]).then(() => {}),
      resolve: (result) => {
        existingItem.resolve(result)
        incomingItem.resolve(result)
      },
      reject: (result) => {
        existingItem.reject(result)
        incomingItem.reject(result)
      },
    }
  }

  private async _runPrecheckForTask(task: ActualTask<Task>): Promise<void> {
    try {
      await this._options.precheck(task)
    } catch (err) {
      if (this._options.retryPrecheckOnFailure) {
        await sleep(this._options.precheckRetryDelay)
        return this._runPrecheckForTask(task)
      }
      throw err
    }
  }
}
