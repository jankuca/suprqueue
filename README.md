# suprqueue

Simple in-memory promise-based task queue with support for pausing, merging tasks, or retrying failed tasks

---

## Features

- Queueing and running tasks one by one
- Queued tasks of the same type can be merged together
- Failed tasks can be retried, with an optional delay, either right away or after other queued tasks
- Pre-check step can be run before every task
- The queue can be paused and later resumed

## Installation

```bash
npm install --save suprqueue
```

## Usage

```typescript
import { Suprqueue } from 'suprqueue'

const queue = new Suprqueue(
  async (task: YourTask): YourResult => {
    console.log('process task', task)
  },
  {
    retryOnFailure: true,
    retryDelay: 1000,
  }
)

const yourResult = await queue.pushTask(yourTask)
```

## Examples

### Simple fetch queue

```typescript
const queue = new Suprqueue((url: string) => {
  const res = await fetch(url)
  console.log('Downloaded', url, await res.json())
})

queue.pushTask('/api/items/1')
queue.pushTask('/api/items/2')
```

### Working with task results

The returned promise is resolved with the tasks result, or its failure

```typescript
const [item1, item2] = await Promise.all([queue.pushTask('/api/items/1'), queue.pushTask('/api/items/2')])

try {
  const item3 = queue.pushTask('/api/items/3')
} catch (err) {
  console.error('Failed to process task', err)
}
```

### Retrying failed tasks

```typescript
const queue = new Suprqueue(
  async (task: YourTask) => {
    // Your logic that can fail/throw
  },
  {
    retryOnFailure: true,
  }
)

const yourResult = await queue.pushTask(yourTask)
```

The retry can also be delayed via the `retryDelay` option.

In case other tasks are queued while processing the task that failed, we can choose whether the retry should be prioritized before the newer tasks (preserving original order) or whether it should be appended to the queue and run afterwards. This is done via the `retryBeforeOtherTasks` option.

### Merging of similar tasks

It can be beneficial to batch tasks together. In such situation, the tasks for which the `key` function returns the same value are merged via the `merge` function. The promises of all merged tasks resolve with the same result.

```typescript
const queue = new Suprqueue(
  async (task: { type: string, ids: Array<string> }) => {
    const results = ids.map((id) => `result:${id}`)
    return results
  },
  {
    key: (task) => task.type,
    merge: (task1, task2) => ({ ...task1, ids: [...task1.ids, ...task2.ids] }),
  }
)

const [appleResults, bananaResults, appleResultsToo] = await Promise.all([
  queue.pushTask({ type: 'apple', ids: ['a', 'b'] }),
  queue.pushTask({ type: 'banana', ids: ['x']})
  queue.pushTask({ type: 'apple', ids: ['c', 'd'] }),
])
assert(appleResults === appleResultsToo)
assert.deepEqual(appleResults, ['result:a', 'result:b', 'result:c', 'result:d'])
assert.deepEqual(bananaResults, ['result:x'])
```

### Connection check before processing tasks

```typescript
const queue = new Suprqueue(
  async (op: DocumentOperation) => {
    const result = await doc.applyOperation(op)
    return { op, result }
  },
  {
    precheck: () => {
      if (!doc.isReady()) {
        throw new Error('Document is not ready to have operations applied.')
      }
    },
    retryPrecheckOnFailure: true,
    precheckRetryDelay: 500,
  }
)
```

### Pausing the queue

There can be many situations in which it is desirable to pause a queue. To prevent race conditions with another process or to remain within some sort of a quota can be two nice examples.

```typescript
const queue = new Suprqueue(
  async (itemId: string) => {
    const res = await fetch(`https://some-api-with-quota/fetch-item/${itemId}`)
    if (res.status === 429) {
      queue.pause()

      const resetAfterSeconds = Number(res.headers.get('RateLimit-Reset')) || 60
      setTimeout(() => {
        queue.resume()
      }, resetAfterSeconds * 1000)

      throw new Error('Rate limit reached')
    }

    saveItem(await res.json())
  },
  {
    retryOnFailure: true,
  }
)

queue.pushTask('a')
queue.pushTask('b')
```

The queue can be paused from within the task handler, the precheck function, or from any other logic.

In-progress tasks are finished when the queue gets paused, although they are not retried on failure – they are only retried later on resume, provided the `retryOnFailure` option is set.

When the queue is paused during the precheck function or the retry delay of a failed task, the task is only processed after the queue is resumed.

## License

[MIT](/LICENSE)
