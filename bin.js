const cluster = require('cluster')
const os = require('os')
const express = require('express')

const port = 3000
const numCPUs = os.cpus().length

// To track active computation tasks
const tasks = {}
let taskIdCounter = 0

// ----------- Master Process ------------
if (cluster.isPrimary) {
  console.log(`Master ${process.pid} is running`)

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork()

    worker.on('message', (msg) => {
      // Handle new sum task from a worker
      if (msg.cmd === 'startTask') {
        const taskId = ++taskIdCounter
        const ranges = splitRange(msg.n, numCPUs)

        tasks[taskId] = {
          originWorkerId: worker.id,
          expectedResults: numCPUs,
          receivedResults: 0,
          total: 0,
        }

        // Send each worker a range to compute
        Object.values(cluster.workers).forEach((w, index) => {
          w.send({
            cmd: 'compute',
            taskId,
            start: ranges[index].start,
            end: ranges[index].end,
          })
        })
      }

      // Handle partial result from worker
      else if (msg.cmd === 'partialResult') {
        const task = tasks[msg.taskId]
        
        print(task)
        if (!task) return

        task.total += msg.result
        task.receivedResults++

        // Once all parts received, send back to requesting worker
        if (task.receivedResults === task.expectedResults) {
          const originWorker = cluster.workers[task.originWorkerId]
          if (originWorker) {
            originWorker.send({
              cmd: 'finalResult',
              taskId: msg.taskId,
              result: task.total,
            })
          }
          delete tasks[msg.taskId] // Clean up
        }
      }
    })
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`)
  })
}

// ----------- Worker Process ------------
else {
  const app = express()
  console.log(`Worker ${process.pid} started`)

  app.get('/', (req, res) => {
    res.send('Hello World!')
  })

  app.get('/api/:n', (req, res) => {
    const n = parseInt(req.params.n)
    const taskId = `${Date.now()}-${process.pid}`
    console.log("taskid "+taskId)

    // Ask master to split and start task
    process.send({
      cmd: 'startTask',
      n,
      taskId,
    })

    // Wait for final result from master
    process.once('message', (msg) => {
      if (msg.cmd === 'finalResult' && msg.taskId === taskId) {
        res.send(`Sum from 1 to ${n} is ${msg.result} (handled by worker ${process.pid})`)
      }
    })
  })

  // Receive compute command from master
  process.on('message', (msg) => {
    if (msg.cmd === 'compute') {
      const { start, end, taskId } = msg
      let result = 0
      for (let i = start; i <= end; i++) {
        result += i
      }

      process.send({
        cmd: 'partialResult',
        result,
        taskId,
      })
    }
  })

  app.listen(port, () => {
    console.log(`Worker listening at http://localhost:${port}`)
  })
}

// ----------- Utility Function ------------
function splitRange(n, parts) {
  const size = Math.floor(n / parts)
  const ranges = []
  let start = 1

  for (let i = 0; i < parts; i++) {
    const end = (i === parts - 1) ? n : start + size - 1
    ranges.push({ start, end })
    start = end + 1
  }

  return ranges
}
