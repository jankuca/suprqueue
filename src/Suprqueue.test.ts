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

  it('should run a task queued during the execution of the previous task after all previously queued task finishes', () => {
    return spec({
      given() {
        let resolveC: (result: any) => void
        const cPromise = new Promise((resolve) => {
          resolveC = resolve
        })

        const log: Array<string> = []
        const queue = new Suprqueue(async (task: string) => {
          log.push(`start ${task}`)
          await sleep(10)
          if (task === 'a') {
            resolveC(queue.pushTask('c'))
          }
          log.push(`end ${task}`)
        })

        return {
          queue,
          log,
          cPromise,
        }
      },
      async perform({ queue, cPromise }) {
        await Promise.all([queue.pushTask('a'), queue.pushTask('b'), cPromise])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'end a', 'start b', 'end b', 'start c', 'end c'])
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

  it('should replace tasks with the same computed key when queued with other items in between while being setup to merge all items', () => {
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
              mergeConsecutiveOnly: false,
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

  it('should not replace tasks with the same computed key when queued with other items in between while being setup to merge consecutive items only', () => {
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
              mergeConsecutiveOnly: true,
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
        expect(log).toEqual(['start a:x', 'end a:x', 'start b:z', 'end b:z', 'start a:y', 'end a:y'])
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

  it('should not merge tasks with the same computed key when the provided merge function throws', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue<{ id: string; items: Array<string> }, void>(
            async (task) => {
              log.push(`start ${task.id}:${task.items}`)
              await sleep(10)
              log.push(`end ${task.id}:${task.items}`)
            },
            {
              key: (task) => task.id,
              merge: () => {
                throw new Error('Failed by the test.')
              },
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
        expect(log).toEqual(['start a:x', 'end a:x', 'start a:y', 'end a:y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should merge tasks with the same computed key using the provided merge function when queued with other items in between while being setup to merge all items', () => {
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
              mergeConsecutiveOnly: false,
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

  it('should not merge tasks with the same computed key when queued with other items in between while being setup to merge consecutive items only', () => {
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
              mergeConsecutiveOnly: true,
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
        expect(log).toEqual(['start a:x', 'end a:x', 'start b:z', 'end b:z', 'start a:y', 'end a:y'])
      },
    })
  })

  it('should merge consecutive tasks with the same computed key when queued with other items in between while being setup to merge consecutive items only', () => {
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
              mergeConsecutiveOnly: true,
            }
          ),
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([
          queue.pushTask({ id: 'a', items: ['x1'] }),
          queue.pushTask({ id: 'a', items: ['x2'] }),
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y1'] }),
          queue.pushTask({ id: 'a', items: ['y2'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x1,x2', 'end a:x1,x2', 'start b:z', 'end b:z', 'start a:y1,y2', 'end a:y1,y2'])
      },
    })
  })

  it('should merge a retried task with the absolute last queued item of the same computed key using the provided merge function with the retried task being considered newer if set up to retry after other tasks', () => {
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
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'start b:z', 'end b:z', 'start a:y,x', 'end a:y,x'])
      },
    })
  })

  it('should not merge a retried task with the absolute last queued item of the same computed key when the provided merge function fails while being set up to retry after other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue<{ id: string; items: Array<string> }, void>(
            async (task) => {
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
              merge: () => {
                throw new Error('Failed by the test.')
              },
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
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start b:z',
          'end b:z',
          'start a:y',
          'end a:y',
          'start a:x',
          'end a:x',
        ])
      },
    })
  })

  it('should merge a retried task with the last queued item of the same computed key with another item following it using the provided merge function with the retried task being considered newer if set up to retry after other tasks and to merge all items', () => {
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
              mergeConsecutiveOnly: false,
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

  it('should not merge a retried task with the last queued item of the same computed key with another item following it when the provided merge function fails while set up to retry after other tasks and to merge all items', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue<{ id: string; items: Array<string> }, void>(
            async (task) => {
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
              merge: () => {
                throw new Error('Failed by the test.')
              },
              mergeConsecutiveOnly: false,
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
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start a:y',
          'end a:y',
          'start a:x',
          'end a:x',
          'start b:z',
          'end b:z',
        ])
      },
    })
  })

  it('should not merge a retried task with the last queued item of the same computed key with another item following it if set up to retry after other tasks and to merge consecutive items only', () => {
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
              mergeConsecutiveOnly: true,
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
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start a:y',
          'end a:y',
          'start b:z',
          'end b:z',
          'start a:x',
          'end a:x',
        ])
      },
    })
  })

  it('should merge a retried task with an immediately following queued item of the same computed key using the provided merge function with the queued task being considered newer if set up to retry before other tasks', () => {
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

  it('should not merge a retried task with an immediately following queued item of the same computed key when the provided merge function fails while being set up to retry before other tasks', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue<{ id: string; items: Array<string> }, void>(
            async (task) => {
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
              merge: () => {
                throw new Error('Failed by the test.')
              },
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
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start a:x',
          'end a:x',
          'start a:y',
          'end a:y',
          'start b:z',
          'end b:z',
        ])
      },
    })
  })

  it('should merge a retried task with the next queued item of the same computed key with another item preceding it using the provided merge function with the queued task being considered newer if set up to retry before other tasks and to merge all items', () => {
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
              mergeConsecutiveOnly: false,
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
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['start a:x', 'error a:x', 'start a:x,y', 'end a:x,y', 'start b:z', 'end b:z'])
      },
    })
  })

  it('should not merge a retried task with the next queued item of the same computed key with another item preceding it when the provided merge function fails while being set up to retry before other tasks and to merge all items', () => {
    return spec({
      given() {
        const log: Array<string> = []
        return {
          queue: new Suprqueue<{ id: string; items: Array<string> }, void>(
            async (task) => {
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
              merge: () => {
                throw new Error('Failed by the test.')
              },
              mergeConsecutiveOnly: false,
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
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start a:x',
          'end a:x',
          'start a:y',
          'end a:y',
          'start b:z',
          'end b:z',
        ])
      },
    })
  })

  it('should not merge a retried task with the next queued item of the same computed key with another item preceding it if set up to retry before other tasks and to merge consecutive items only', () => {
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
              mergeConsecutiveOnly: true,
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
          queue.pushTask({ id: 'b', items: ['z'] }),
          queue.pushTask({ id: 'a', items: ['y'] }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual([
          'start a:x',
          'error a:x',
          'start a:x',
          'end a:x',
          'start b:z',
          'end b:z',
          'start a:y',
          'end a:y',
        ])
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
              mergeConsecutiveOnly: false,
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

  it('should resolve promises of merged consecutive tasks with the same computed key with the merged task result', () => {
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
            queue.pushTask({ id: 'b', items: ['y'] }),
          ]),
        }
      },
      expect({ results }) {
        expect(results).toEqual(['result a:x,y', 'result a:x,y', 'result b:x,y', 'result b:x,y'])
      },
    })
  })

  it('should resolve promises of merged non-consecutive tasks with the same computed key with the merged task result when setup to merge all items', () => {
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
              mergeConsecutiveOnly: false,
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

  it('should resolve promises of merged consecutive same-key task groups with other items queued in between with the merged task result when setup to only merge consecutive items', () => {
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
              mergeConsecutiveOnly: true,
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
        expect(results).toEqual(['result a:x,y', 'result a:x,y', 'result b:x', 'result a:z', 'result b:y'])
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
              mergeConsecutiveOnly: false,
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

  it('should claim it is drained after running a single task with no other tasks queued', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            log.push(`end ${task}`)
          },
          {
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([await queue.pushTask('a'), sleep(10)])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'end a', 'drain'])
      },
    })
  })

  it('should claim it is drained after running multiple task in a row when no other tasks are queued', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            log.push(`end ${task}`)
          },
          {
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([queue.pushTask('a'), await queue.pushTask('b'), sleep(10)])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'end a', 'start b', 'end b', 'drain'])
      },
    })
  })

  it('should claim it is drained only after also running tasks queued during previous task execution', () => {
    return spec({
      given() {
        let resolveC: (result: any) => void
        const bPromise = new Promise((resolve) => {
          resolveC = resolve
        })

        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            if (task === 'a') {
              resolveC(queue.pushTask('b'))
            }
            log.push(`end ${task}`)
          },
          {
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
          bPromise,
        }
      },
      async perform({ queue, bPromise }) {
        await Promise.all([queue.pushTask('a'), await bPromise, sleep(10)])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'end a', 'start b', 'end b', 'drain'])
      },
    })
  })

  it('should claim it is drained only after failed task retries finish', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            if (!log.includes('error a')) {
              log.push('error a')
              throw new Error('error a')
            }
            log.push(`end ${task}`)
          },
          {
            retryOnFailure: true,
            retryDelay: 10,
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([await queue.pushTask('a'), sleep(10)])
      },
      expect({ log }) {
        expect(log).toEqual(['start a', 'error a', 'start a', 'end a', 'drain'])
      },
    })
  })

  it('should claim it is drained only after failed precheck retries and the actual tasks finish', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`start ${task}`)
            await sleep(10)
            log.push(`end ${task}`)
          },
          {
            retryPrecheckOnFailure: true,
            precheckRetryDelay: 10,
            precheck: async (task: string) => {
              log.push(`precheck ${task}`)
              if (!log.includes('error precheck a')) {
                log.push('error precheck a')
                throw new Error('error precheck a')
              }
            },
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await Promise.all([await queue.pushTask('a'), sleep(10)])
      },
      expect({ log }) {
        expect(log).toEqual(['precheck a', 'error precheck a', 'precheck a', 'start a', 'end a', 'drain'])
      },
    })
  })

  it('should wait for the set task delay before running the first task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(task)
          },
          {
            taskDelay: 10,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a'),
          sleep(9).then(() => {
            log.push('delay')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay', 'a'])
      },
    })
  })

  it('should wait for the task delay calculated for the task before running the first task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { id: string; delay: number }) => {
            log.push(task.id)
          },
          {
            taskDelay: (task) => task.delay,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ id: 'a', delay: 10 }),
          sleep(9).then(() => {
            log.push('delay')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay', 'a'])
      },
    })
  })

  it('should keep merging tasks while waiting for the set task delay', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { type: string; items: Array<string> }) => {
            log.push(`${task.type}:${task.items}`)
          },
          {
            key: (task) => task.type,
            merge: (existingTask, incomingTask) => ({
              type: existingTask.type,
              items: [...existingTask.items, ...incomingTask.items],
            }),
            taskDelay: 20,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ type: 'a', items: ['x'] }),
          sleep(10).then(() => queue.pushTask({ type: 'a', items: ['y'] })),
          sleep(19).then(() => {
            log.push('delay will elapse')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay will elapse', 'a:x,y'])
      },
    })
  })

  it('should keep merging tasks while waiting for the task delay calulated for the previous task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { type: string; items: Array<string>; delay: number }) => {
            log.push(`${task.type}:${task.items}`)
          },
          {
            key: (task) => task.type,
            merge: (existingTask, incomingTask) => ({
              type: existingTask.type,
              items: [...existingTask.items, ...incomingTask.items],
              delay: existingTask.delay,
            }),
            taskDelay: (task) => task.delay,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ type: 'a', items: ['x'], delay: 20 }),
          sleep(10).then(() => queue.pushTask({ type: 'a', items: ['y'], delay: 0 })),
          sleep(19).then(() => {
            log.push('delay will elapse')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay will elapse', 'a:x,y'])
      },
    })
  })

  it('should process tasks queued after the task delay elapses separately from previously merged ones', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { type: string; items: Array<string> }) => {
            log.push(`${task.type}:${task.items}`)
          },
          {
            key: (task) => task.type,
            merge: (existingTask, incomingTask) => ({
              type: existingTask.type,
              items: [...existingTask.items, ...incomingTask.items],
            }),
            taskDelay: 20,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ type: 'a', items: ['x'] }),
          sleep(10).then(() => queue.pushTask({ type: 'a', items: ['y'] })),
          sleep(25).then(() => {
            log.push('delay elapsed')
          }),
          sleep(30).then(() => queue.pushTask({ type: 'a', items: ['z'] })),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['a:x,y', 'delay elapsed', 'a:z'])
      },
    })
  })

  it('should process tasks queued after the task delay elapses separately from previously merged ones', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { type: string; items: Array<string>; delay: number }) => {
            log.push(`${task.type}:${task.items}`)
          },
          {
            key: (task) => task.type,
            merge: (existingTask, incomingTask) => ({
              type: existingTask.type,
              items: [...existingTask.items, ...incomingTask.items],
              delay: incomingTask.delay,
            }),
            taskDelay: (task) => task.delay,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ type: 'a', items: ['x'], delay: 40 }),
          sleep(20).then(() => queue.pushTask({ type: 'a', items: ['y'], delay: 0 })),
          sleep(50).then(() => {
            log.push('delay elapsed')
          }),
          sleep(60).then(() => queue.pushTask({ type: 'a', items: ['z'], delay: 0 })),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['a:x,y', 'delay elapsed', 'a:z'])
      },
    })
  })

  it('should not wait for the set task delay when retrying a failed task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`${task}`)
            if (!log.includes(`error ${task}`)) {
              log.push(`error ${task}`)
              throw new Error(`error ${task}`)
            }
          },
          {
            taskDelay: 40,
            retryOnFailure: true,
            retryDelay: 0,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a'),
          sleep(35).then(() => {
            log.push('delay')
          }),
          sleep(60).then(() => {
            log.push('stop')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay', 'a', 'error a', 'a', 'stop'])
      },
    })
  })

  it('should not wait for the task delay calculated for the task when retrying a failed task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { id: string; delay: number }) => {
            log.push(`${task.id}`)
            if (!log.includes(`error ${task.id}`)) {
              log.push(`error ${task.id}`)
              throw new Error(`error ${task.id}`)
            }
          },
          {
            taskDelay: (task) => task.delay,
            retryOnFailure: true,
            retryDelay: 0,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ id: 'a', delay: 40 }),
          sleep(35).then(() => {
            log.push('delay')
          }),
          sleep(60).then(() => {
            log.push('stop')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['delay', 'a', 'error a', 'a', 'stop'])
      },
    })
  })

  it('should only wait for the set retry delay and not the set task delay when retrying a failed task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(`${task}`)
            if (!log.includes(`error ${task}`)) {
              log.push(`error ${task}`)
              throw new Error(`error ${task}`)
            }
          },
          {
            taskDelay: 40,
            retryOnFailure: true,
            retryDelay: 20,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask('a'),
          sleep(35).then(() => {
            log.push('task delay')
          }),
          sleep(55).then(() => {
            log.push('retry delay')
          }),
          sleep(75).then(() => {
            log.push('second task delay')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['task delay', 'a', 'error a', 'retry delay', 'a', 'second task delay'])
      },
    })
  })

  it('should only wait for the set retry delay and not the set task delay when retrying a failed task into which a new task is merged during the retry delay', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: { type: string; items: Array<string> }) => {
            log.push(`${task.type}:${task.items}`)
            if (`${task.type}:${task.items[0]}` === 'a:x' && !log.includes(`error a:x`)) {
              log.push(`error a:x`)
              throw new Error(`error a:x`)
            }
          },
          {
            key: (task) => task.type,
            merge: (existingTask, incomingTask) => ({
              type: existingTask.type,
              items: [...existingTask.items, ...incomingTask.items],
            }),
            taskDelay: 40,
            retryOnFailure: true,
            retryDelay: 20,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await Promise.all([
          queue.pushTask({ type: 'a', items: ['x'] }),
          sleep(35).then(() => {
            log.push('task delay')
          }),
          sleep(50).then(() => {
            log.push('queue y')
            return queue.pushTask({ type: 'a', items: ['y'] })
          }),
          sleep(55).then(() => {
            log.push('retry delay')
          }),
          sleep(75).then(() => {
            log.push('second task delay')
          }),
        ])
      },
      expect({ log }) {
        expect(log).toEqual(['task delay', 'a:x', 'error a:x', 'queue y', 'retry delay', 'a:x,y', 'second task delay'])
      },
    })
  })

  it('should fail a task when canceled during its processing', () => {
    return spec({
      given() {
        const queue = new Suprqueue(
          async (task: string) => {
            queue.cancelTasks(task)
          },
          {
            key: (task) => task,
          }
        )

        return {
          queue,
        }
      },
      async perform({ queue }) {
        return { result: (await Promise.allSettled([queue.pushTask('a')]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) })
      },
    })
  })

  it('should fail a task when canceled before its processing starts', () => {
    return spec({
      given() {
        const queue = new Suprqueue(async (task: string) => {}, {
          key: (task) => task,
        })

        return {
          queue,
        }
      },
      async perform({ queue }) {
        const taskPromise = queue.pushTask('a')
        queue.cancelTasks('a')
        return { result: (await Promise.allSettled([taskPromise]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) })
      },
    })
  })

  it('should fail all tasks of the specified key that are not processing', () => {
    return spec({
      given() {
        const queue = new Suprqueue(async (task: string) => {}, {
          key: (task) => task,
        })

        return {
          queue,
        }
      },
      async perform({ queue }) {
        const taskPromises = [queue.pushTask('a'), queue.pushTask('a'), queue.pushTask('a')]
        queue.cancelTasks('a')
        return { results: await Promise.allSettled(taskPromises) }
      },
      expect({ results }) {
        expect(results).toEqual([
          { status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) },
          { status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) },
          { status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) },
        ])
      },
    })
  })

  it('should not fail the currently running task when a task that already finished is canceled', () => {
    return spec({
      given() {
        const queue = new Suprqueue(async (task: string) => task, {
          key: (task) => task,
        })

        return {
          queue,
        }
      },
      async perform({ queue }) {
        await Promise.allSettled([queue.pushTask('a'), queue.pushTask('b')])
        const taskAPromise = queue.pushTask('a')
        const taskBPromise = queue.pushTask('b')
        await taskAPromise
        queue.cancelTasks('a')
        return { result: (await Promise.allSettled([taskBPromise]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'fulfilled', value: 'b' })
      },
    })
  })

  it('should not retry a task canceled during its processing even when configured to retry on failure', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string) => {
            log.push(task)
            queue.cancelTasks(task)
          },
          {
            key: (task) => task,
            retryOnFailure: true,
            onDrain: () => {
              log.push('drain')
            },
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        const taskPromise = queue.pushTask('a')
        await Promise.allSettled([taskPromise])
        // NOTE: wait for drain
        await sleep(0)
      },
      expect({ log }) {
        expect(log).toEqual(['a', 'drain'])
      },
    })
  })

  it('should provide the task handler with the abort signal for the task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string, signal: AbortSignal | null) => {
            expect(signal).toBeTruthy()
            signal?.addEventListener('abort', () => {
              log.push('aborted')
            })
            queue.cancelTasks('a')
            await sleep(10)
            log.push('done')
          },
          {
            key: (task) => task,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        const taskPromise = queue.pushTask('a')
        log.push('abort')
        await Promise.allSettled([taskPromise])
      },
      expect({ log }) {
        expect(log).toEqual(['abort', 'aborted', 'done'])
      },
    })
  })

  it('should not abort the signal for a task when canceled after finishing', () => {
    return spec({
      given() {
        const log: Array<string> = []
        const queue = new Suprqueue(
          async (task: string, signal: AbortSignal | null) => {
            expect(signal).toBeTruthy()
            signal?.addEventListener('abort', () => {
              log.push('aborted')
            })
            log.push('done')
          },
          {
            key: (task) => task,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue, log }) {
        await queue.pushTask('a')
        log.push('abort')
        queue.cancelTasks('a')
        await sleep(10)
      },
      expect({ log }) {
        expect(log).toEqual(['done', 'abort'])
      },
    })
  })

  it('should provide the task handler with a fresh abort signal when retrying a failed task', () => {
    return spec({
      given() {
        const log: Array<string> = []
        let taskCounter = 0

        const queue = new Suprqueue(
          async (task: string, signal: AbortSignal | null) => {
            const taskNumber = ++taskCounter
            log.push(`start ${taskNumber}`)

            expect(signal).toBeTruthy()
            signal?.addEventListener('abort', () => {
              log.push(`aborted ${taskCounter}`)
            })

            // NOTE: The first attempt fails to trigger a retry, the second attempt cancels itself to test signals.
            if (taskNumber === 1) {
              log.push(`fail ${taskNumber}`)
              throw new Error('Failed')
            } else {
              log.push(`abort ${taskNumber}`)
              queue.cancelTasks('a')
            }
          },
          {
            key: (task) => task,
            retryOnFailure: true,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        await Promise.allSettled([queue.pushTask('a')])
      },
      expect({ log }) {
        expect(log).toEqual(['start 1', 'fail 1', 'start 2', 'abort 2', 'aborted 2'])
      },
    })
  })

  it('should fail a task when canceled during its processing if its task handler throws the AbortError', () => {
    return spec({
      given() {
        const queue = new Suprqueue(
          async (task: string, abortSignal: AbortSignal | null) => {
            expect(abortSignal).toBeTruthy()
            queue.cancelTasks(task)
            abortSignal?.throwIfAborted()
          },
          {
            key: (task) => task,
          }
        )

        return {
          queue,
        }
      },
      async perform({ queue }) {
        return { result: (await Promise.allSettled([queue.pushTask('a')]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'rejected', reason: expect.objectContaining({ name: 'AbortError' }) })
      },
    })
  })

  it('should fail a task when canceled during its processing if its task handler throws a custom error on abortion', () => {
    return spec({
      given() {
        const queue = new Suprqueue(
          async (task: string, abortSignal: AbortSignal | null) => {
            expect(abortSignal).toBeTruthy()
            queue.cancelTasks(task)
            if (abortSignal?.aborted) {
              throw new Error('Custom error')
            }
          },
          {
            key: (task) => task,
          }
        )

        return {
          queue,
        }
      },
      async perform({ queue }) {
        return { result: (await Promise.allSettled([queue.pushTask('a')]))[0] }
      },
      expect({ result }) {
        expect(result).toEqual({ status: 'rejected', reason: expect.objectContaining({ message: 'Custom error' }) })
      },
    })
  })

  it('should retry a task canceled during its processing if its task handler throws a custom error on abortion', () => {
    return spec({
      given() {
        const log: Array<string> = []
        let taskCounter = 0

        const queue = new Suprqueue(
          async (task: string, abortSignal: AbortSignal | null) => {
            const taskNumber = ++taskCounter
            log.push(`${task} ${taskNumber}`)

            // NOTE: Only canceling on the first attempt to avoid an infinite retry loop.
            if (taskNumber === 1) {
              expect(abortSignal).toBeTruthy()
              queue.cancelTasks(task)
              if (abortSignal?.aborted) {
                throw new Error('Custom error')
              }
            }
          },
          {
            key: (task) => task,
            retryOnFailure: true,
          }
        )

        return {
          queue,
          log,
        }
      },
      async perform({ queue }) {
        return { result: (await Promise.allSettled([queue.pushTask('a')]))[0] }
      },
      expect({ log }) {
        expect(log).toEqual(['a 1', 'a 2'])
      },
    })
  })
})
