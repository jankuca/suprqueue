import { spec } from 'spec-phase'

import { Suprqueue } from './Suprqueue'

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(() => resolve(), ms))

describe('Suprqueue', () => {
  it('should run a task', () => {
    return spec({
      given() {
        const processedTasks: Array<string> = []
        return {
          queue: new Suprqueue((task: string) => {
            processedTasks.push(task)
          }),
          processedTasks,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('test')
      },
      expect({ processedTasks }) {
        expect(processedTasks).toEqual(['test'])
      },
    })
  })

  it('should run multiple different tasks queued one right after another', () => {
    return spec({
      given() {
        const processedTasks: Array<string> = []
        return {
          queue: new Suprqueue((task: string) => {
            processedTasks.push(task)
          }),
          processedTasks,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b')])
      },
      expect({ processedTasks }) {
        expect(processedTasks).toEqual(['a', 'b'])
      },
    })
  })

  it('should run a task queued after the previously queued tasks are finished', () => {
    return spec({
      given() {
        const processedTasks: Array<string> = []
        return {
          queue: new Suprqueue((task: string) => {
            processedTasks.push(task)
          }),
          processedTasks,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b')])
        await queue.pushTask('c')
      },
      expect({ processedTasks }) {
        expect(processedTasks).toEqual(['a', 'b', 'c'])
      },
    })
  })

  it('should wait for a task to finish before running another', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            log.push(`end ${task}`)
          }),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b')])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'end a', 'start b', 'end b'])
      },
    })
  })

  it('should resolve promises of each task with their respective results after finish', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(async (task: string) => {
            await sleep(10)
            log.push(`end ${task}`)
            return `result ${task}`
          }),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a').then((result) => log.push(result)),
          queue.pushTask('b').then((result) => log.push(result)),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['end a', 'result a', 'end b', 'result b'])
      },
    })
  })

  it('should resolve promises of each task with their respective results before running other jobs', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            return `result ${task}`
          }),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a').then((result) => log.push(result)),
          queue.pushTask('b').then((result) => log.push(result)),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'result a', 'start b', 'result b'])
      },
    })
  })

  it('should propagate task failure when retry is not enabled', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              throw new Error(`error ${task}`)
            },
            {
              retryOnFailure: false,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([queue.pushTask('a').catch((error) => log.push(error.message))])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a'])
      },
    })
  })

  it('should retry a failed task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              if (!log.includes(`error ${task}`)) {
                log.push(`error ${task}`)
                throw new Error(`error ${task}`)
              }
            },
            {
              retryOnFailure: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a')])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'start a'])
      },
    })
  })

  it('should retry a failed task after running other queued tasks when setup to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              if (task === 'a' && !log.includes(`error ${task}`)) {
                log.push(`error ${task}`)
                throw new Error(`error ${task}`)
              }
              log.push(`end ${task}`)
            },
            {
              retryOnFailure: true,
              retryBeforeOtherTasks: false,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b')])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'start b', 'end b', 'start a', 'end a'])
      },
    })
  })

  it('should retry a failed task before running other queued tasks when setup to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              if (task === 'a' && !log.includes(`error ${task}`)) {
                log.push(`error ${task}`)
                throw new Error(`error ${task}`)
              }
              log.push(`end ${task}`)
            },
            {
              retryOnFailure: true,
              retryBeforeOtherTasks: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b')])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'start a', 'end a', 'start b', 'end b'])
      },
    })
  })

  it('should wait before retring a failed task but run other tasks with no delay when set up to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              if (task === 'a' && !log.includes(`error ${task}`)) {
                log.push(`error ${task}`)
                throw new Error(`error ${task}`)
              }
              log.push(`end ${task}`)
            },
            {
              retryOnFailure: true,
              retryBeforeOtherTasks: false,
              retryDelay: 1000,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b'), sleep(500).then(() => log.push('waited'))])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'start b', 'end b', 'waited', 'start a', 'end a'])
      },
    })
  })

  it('should wait before retrying a failed task without running other tasks when setup to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(`start ${task}`)
              if (task === 'a' && !log.includes(`error ${task}`)) {
                log.push(`error ${task}`)
                throw new Error(`error ${task}`)
              }
              log.push(`end ${task}`)
            },
            {
              retryOnFailure: true,
              retryBeforeOtherTasks: true,
              retryDelay: 1000,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b'), sleep(500).then(() => log.push('waited'))])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'waited', 'start a', 'end a', 'start b', 'end b'])
      },
    })
  })

  it('should replace tasks with the same computed key when queued in consecutive sequence by default', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; value: string }) => {
              log.push(`start ${task.id}:${task.value}`)
              await sleep(10)
              log.push(`end ${task.id}:${task.value}`)
            },
            {
              key: (task) => task.id,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', value: 'x' }),
          queue.pushTask({ id: 'a', value: 'y' }),
          queue.pushTask({ id: 'b', value: 'z' }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:y', 'end a:y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should replace tasks with the same computed key when queued with other items in between by default', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; value: string }) => {
              log.push(`start ${task.id}:${task.value}`)
              await sleep(10)
              log.push(`end ${task.id}:${task.value}`)
            },
            {
              key: (task) => task.id,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', value: 'x' }),
          queue.pushTask({ id: 'b', value: 'z' }),
          queue.pushTask({ id: 'a', value: 'y' }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:y', 'end a:y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should merge tasks with the same computed key using the provided merge function when queued in consecutive sequence', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
          queue.pushTask({ id: 'b', items: ['z'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x,y', 'end a:x,y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should merge tasks with the same computed key using the provided merge function when queued with other items in between', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x,y', 'end a:x,y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should merge a retried task with the last queued item of the same computed key using the provided merge function with the retried task being considered newer if set up to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              if (`${task.id}:${task.items}` === 'a:x' && !log.includes(`error a:x`)) {
                log.push(`error a:x`)
                throw new Error(`error a:x`)
              }
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: false,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          await sleep(0), // NOTE: Less than task length.
          queue.pushTask({ id: 'a', items: ['y'] }),
          queue.pushTask({ id: 'b', items: ['z'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'start a:y,x', 'end a:y,x', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should merge a retried task with the next queued item of the same computed key using the provided merge function with the queued task being considered newer if set up to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              if (`${task.id}:${task.items}` === 'a:x' && !log.includes(`error a:x`)) {
                log.push(`error a:x`)
                throw new Error(`error a:x`)
              }
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          await sleep(0), // NOTE: Less than task length.
          queue.pushTask({ id: 'a', items: ['y'] }),
          queue.pushTask({ id: 'b', items: ['z'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'start a:x,y', 'end a:x,y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should wait before retrying a task merged with the next queued item of the same computed key if set up to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              if (`${task.id}:${task.items}` === 'a:x' && !log.includes(`error a:x`)) {
                log.push(`error a:x`)
                throw new Error(`error a:x`)
              }
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: true,
              retryDelay: 1000,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          await sleep(0), // NOTE: Less than task length.
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
          sleep(500).then(() => log.push('waited')),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'waited', 'start a:x,y', 'end a:x,y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should wait before retrying a task merged with the next queued item of the same computed key but run other tasks if set up to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              if (`${task.id}:${task.items}` === 'a:x' && !log.includes(`error a:x`)) {
                log.push(`error a:x`)
                throw new Error(`error a:x`)
              }
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: false,
              retryDelay: 1000,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x'] }),
          await sleep(0), // NOTE: Less than task length.
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
          sleep(500).then(() => log.push('waited')),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'start b:z', 'end b:z', 'waited', 'start a:y,x', 'end a:y,x'])
      },
    })
  })

  it('should resolve promises of merged tasks with the same computed key with the merged task result', () => {
    return spec({
      given() {
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              return `result ${task.id}:${task.items}`
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
            }
          ),
        }
      },
      async perform({ queue }) {
        return {
          results: await Promise.all([
            queue.pushTask({ id: 'a', items: ['x'] }),
            queue.pushTask({ id: 'a', items: ['y'] }),
            queue.pushTask({ id: 'b', items: ['x'] }),
            queue.pushTask({ id: 'a', items: ['z'] }),
            queue.pushTask({ id: 'b', items: ['y'] }),
          ]),
        }
      },
      expect({ results }) {
        expect(results).toEqual(['result a:x,y,z', 'result a:x,y,z', 'result b:x,y', 'result a:x,y,z', 'result b:x,y'])
      },
    })
  })

  it('should resolve promises of retried and merged tasks with the same computed key with the merged task result when configured to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              await sleep(10)
              if (!log.includes(`${task.id}:${task.items}`)) {
                log.push(`${task.id}:${task.items}`)
                throw new Error(`${task.id}:${task.items}`)
              }
              return `result ${task.id}:${task.items}`
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: false,
            }
          ),
        }
      },
      async perform({ queue }) {
        return {
          results: await Promise.all([
            queue.pushTask({ id: 'a', items: ['x'] }),
            await sleep(0).then(() => 'sleep'), // NOTE: Less than task length.
            queue.pushTask({ id: 'b', items: ['z'] }),
            queue.pushTask({ id: 'a', items: ['y'] }),
          ]),
        }
      },
      expect({ results }) {
        expect(results).toEqual(['result a:y,x', 'sleep', 'result b:z', 'result a:y,x'])
      },
    })
  })

  it('should resolve promises of retried and merged tasks with the same computed key with the merged task result when configured to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: { id: string; items: Array<string> }) => {
              await sleep(10)
              if (!log.includes(`${task.id}:${task.items}`)) {
                log.push(`${task.id}:${task.items}`)
                throw new Error(`${task.id}:${task.items}`)
              }
              return `result ${task.id}:${task.items}`
            },
            {
              key: (task) => task.id,
              merge: (existingTask, incomingTask) => ({
                id: existingTask.id,
                items: [...existingTask.items, ...incomingTask.items],
              }),
              retryOnFailure: true,
              retryBeforeOtherTasks: true,
            }
          ),
        }
      },
      async perform({ queue }) {
        return {
          results: await Promise.all([
            queue.pushTask({ id: 'a', items: ['x'] }),
            await sleep(0).then(() => 'sleep'), // NOTE: Less than task length.
            queue.pushTask({ id: 'b', items: ['z'] }),
            queue.pushTask({ id: 'a', items: ['y'] }),
          ]),
        }
      },
      expect({ results }) {
        expect(results).toEqual(['result a:x,y', 'sleep', 'result b:z', 'result a:x,y'])
      },
    })
  })

  it('should run the registered precheck before running a task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
            },
            {
              precheck: (task) => {
                log.push(`precheck ${task}`)
              },
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('task')
      },
      expect({ log }) {
        expect(log).toEqual(['precheck task', 'task'])
      },
    })
  })

  it('should wait for the precheck to finish before running a task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
            },
            {
              precheck: async (task) => {
                log.push(`precheck ${task} start`)
                await sleep(10)
                log.push(`precheck ${task} end`)
              },
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('task')
      },
      expect({ log }) {
        expect(log).toEqual(['precheck task start', 'precheck task end', 'task'])
      },
    })
  })

  it('should also run the registered precheck before a retry of a task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
              if (!log.includes('error')) {
                log.push('error')
                throw new Error('error')
              }
            },
            {
              precheck: (task) => {
                log.push(`precheck ${task}`)
              },
              retryOnFailure: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('task')
      },
      expect({ log }) {
        expect(log).toEqual(['precheck task', 'task', 'error', 'precheck task', 'task'])
      },
    })
  })

  it('should fail a task when the registered precheck fails for the task when not set up to retry the precheck', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(async (task: string) => {}, {
            precheck: async () => {
              throw new Error('precheck error')
            },
          }),
          log,
        }
      },
      async perform({ queue }) {
        return { result: (await Promise.allSettled([queue.pushTask('task')]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'rejected', reason: new Error('precheck error') })
      },
    })
  })

  it('should retry the registered precheck on failure when set up to retry the precheck', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
            },
            {
              precheck: async (task) => {
                log.push(`precheck ${task}`)
                if (!log.includes(`precheck ${task} error`)) {
                  log.push(`precheck ${task} error`)
                  throw new Error('precheck error')
                }
              },
              retryPrecheckOnFailure: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('task')
      },
      expect({ log }) {
        expect(log).toEqual(['precheck task', 'precheck task error', 'precheck task', 'task'])
      },
    })
  })

  it('should wait before retrying the registered precheck on failure when set up to retry the precheck with a delay', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
            },
            {
              precheck: async (task) => {
                log.push(`precheck ${task}`)
                if (!log.includes(`precheck ${task} error`)) {
                  log.push(`precheck ${task} error`)
                  throw new Error('precheck error')
                }
              },
              retryPrecheckOnFailure: true,
              precheckRetryDelay: 1000,
            }
          ),
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([queue.pushTask('task'), sleep(500).then(() => log.push('waited'))])
      },
      expect({ log }) {
        expect(log).toEqual(['precheck task', 'precheck task error', 'waited', 'precheck task', 'task'])
      },
    })
  })

  it('should not run any queued task when paused right after tasks are queued', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(async (task: string) => {
            log.push(task)
          }),
          log,
        }
      },
      async perform({ queue }) {
        void queue.pushTask('a')
        void queue.pushTask('b')
        queue.pause()
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual([])
      },
    })
  })

  it('should not run any other tasks after being paused during running a task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(task)
          queue.pause()
        })

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        void queue.pushTask('a')
        void queue.pushTask('b')
        await sleep(100)
      },
      expect({ log }) {
        expect(log).toEqual(['a'])
      },
    })
  })

  it('should not run the task when being paused during running its precheck', () => {
    return spec({
      given() {
        const queue = new Suprqueue(async (task: string) => {
          queue.pause()
          return `result ${task}`
        })

        return {
          queue,
        }
      },
      async perform({ queue }) {
        const promise = queue.pushTask('a')
        void queue.pushTask('b')
        await sleep(10)
        return { result: await promise }
      },
      expect({ result }) {
        expect(result).toEqual('result a')
      },
    })
  })

  it('should run the precheck again and run the task when resumed after being paused during running its precheck', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(task)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task} start`)
              if (!log.includes('resume')) {
                queue.pause()
                await sleep(10)
              }
              log.push(`precheck ${task} end`)
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        void queue.pushTask('a')
        await sleep(20)
        log.push('resume')
        queue.resume()
        await sleep(20)
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a start', 'precheck a end', 'resume', 'precheck a start', 'precheck a end', 'a'])
      },
    })
  })

  it('should not run the precheck again and run the task after being paused and resumed back during running its precheck', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(task)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task} start`)
              queue.pause()
              await sleep(10)
              queue.resume()
              log.push(`precheck ${task} end`)
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await queue.pushTask('a')
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a start', 'precheck a end', 'a'])
      },
    })
  })

  it('should finish the running task when being paused during running a task', () => {
    return spec({
      given() {
        const queue = new Suprqueue(async (task: string) => {
          queue.pause()
          return `result ${task}`
        })

        return {
          queue,
        }
      },
      async perform({ queue }) {
        const promise = queue.pushTask('a')
        void queue.pushTask('b')
        await sleep(10)
        return { result: await promise }
      },
      expect({ result }) {
        expect(result).toEqual('result a')
      },
    })
  })

  it('should not run the registered precheck when paused right after tasks are queued', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue(
            async (task: string) => {
              log.push(task)
            },
            {
              precheck: (task) => {
                log.push(`precheck ${task}`)
              },
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        void queue.pushTask('a')
        void queue.pushTask('b')
        queue.pause()
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual([])
      },
    })
  })

  it('should not run a task when paused while running its precheck', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(task)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task} start`)
              queue.pause()
              await sleep(10)
              log.push(`precheck ${task} end`)
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        void queue.pushTask('a')
        await sleep(20)
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a start', 'precheck a end'])
      },
    })
  })

  it('should not run the precheck of a retried task nor the retried task when paused during the previous run of the task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            queue.pause()
            if (!log.includes(`error ${task}`)) {
              log.push(`error ${task}`)
              throw new Error(`error ${task}`)
            }
            log.push(`end ${task}`)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task}`)
            },
            retryOnFailure: true,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        void queue.pushTask('a')
        await sleep(20)
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a', 'start a', `error a`])
      },
    })
  })

  it('should not run the precheck of a retried task nor the retried task when paused during the retry delay of the task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            if (!log.includes(`error ${task}`)) {
              log.push(`error ${task}`)
              throw new Error(`error ${task}`)
            }
            log.push(`end ${task}`)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task}`)
            },
            retryOnFailure: true,
            retryDelay: 50,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        void queue.pushTask('a')
        sleep(20).then(() => {
          log.push('pause')
          queue.pause()
        })
        await sleep(100)
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a', 'start a', `error a`, `pause`])
      },
    })
  })

  it('should run the precheck of a retried task and the retried task when resumed after being paused during the retry delay of the task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            if (!log.includes(`error ${task}`)) {
              log.push(`error ${task}`)
              throw new Error(`error ${task}`)
            }
            log.push(`end ${task}`)
          },
          {
            precheck: async (task) => {
              log.push(`precheck ${task}`)
            },
            retryOnFailure: true,
            retryDelay: 50,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        void queue.pushTask('a')
        sleep(20).then(() => {
          log.push('pause')
          queue.pause()
        })
        await sleep(100)
        log.push('resume')
        queue.resume()
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a', 'start a', `error a`, `pause`, 'resume', 'precheck a', 'start a', 'end a'])
      },
    })
  })

  it('should not run tasks queued after being paused while idle', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(task)
        })

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await queue.pushTask('a')
        log.push('pause')
        queue.pause()
        void queue.pushTask('b')
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual(['a', 'pause'])
      },
    })
  })

  it('should run tasks queued after being paused while idle when resumed', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(task)
        })

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await queue.pushTask('a')
        log.push('pause')
        queue.pause()
        void queue.pushTask('b')
        void queue.pushTask('c')
        await sleep(10)

        log.push('resume')
        queue.resume()
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual(['a', 'pause', 'resume', 'b', 'c'])
      },
    })
  })

  it('should not run any tasks after being paused right after finishing a task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(task)
        })

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        const promise = queue.pushTask('a').then(() => {
          log.push('pause')
          queue.pause()
        })
        void queue.pushTask('b')
        await promise
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual(['a', 'pause'])
      },
    })
  })

  it('should run remaining queued tasks after being paused right after finishing a task when resumed', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(task)
        })

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a').then(() => {
            log.push('pause')
            queue.pause()
          }),
          queue.pushTask('b'),
          await sleep(10),
          log.push('resume'),
          queue.resume(),
          await sleep(10),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['a', 'pause', 'resume', 'b'])
      },
    })
  })
})
