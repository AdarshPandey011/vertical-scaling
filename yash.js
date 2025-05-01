const cluster = require('cluster')
const os = require('os')
const express = require('express')
 
const port = 3003
const cp_count = os.cpus().length
 
const tasks = {}
let taskIdCounter = 0
 
// ---------- Master Process ----------
if (cluster.isPrimary) {
  console.log(`[MASTER ${process.pid}] Primary process is running`)
 
  // Fork workers
  for (let i = 0; i < cp_count; i++) {
    const worker = cluster.fork()
    console.log(`[MASTER ${process.pid}] Forked Worker ${worker.id} (PID: ${worker.process.pid})`)
 
    // Handle messages from workers
    worker.on('message', (msg) => {
      if (msg.cmd === 'startTask') {
        const taskId = msg.taskId
        const n = msg.n
        const requesterId = worker.id
      
        console.log('requesterId',requesterId)
 
        console.log(`[MASTER ${process.pid}] Received task request from Worker ${requesterId} to sum 1 to ${n} (Task ID: ${taskId})`)
 
        const ranges = splitRange(n, cp_count)
        tasks[taskId] = {
          originWorkerId: requesterId,
          expectedResults: cp_count,
          received: 0,
          total: 0,
        }
 
        // Distribute computation ranges to all workers
        Object.values(cluster.workers).forEach((w, index) => {
          const range = ranges[index]
          console.log(`[MASTER ${process.pid}] Assigning range ${range.start}-${range.end} to Worker ${w.id}`)
          w.send({
            cmd: 'compute',
            taskId,
            start: range.start,
            end: range.end,
          })
        })
      }
 
      // Collect partial results
      else if (msg.cmd === 'partialResult') {
        const task = tasks[msg.taskId]
        console.log('task.taskId '+task.taskId)
        if (!task) return
 
        console.log(`[MASTER ${process.pid}] Received partial result ${msg.result} for Task ${msg.taskId}`)
 
        task.total += msg.result
        task.received++
 
        // Send final result if all parts are received
        if (task.received === task.expectedResults) {
          const originWorker = cluster.workers[task.originWorkerId]
          console.log(`[MASTER ${process.pid}] All results received. Sending final result ${task.total} to Worker ${task.originWorkerId}`)
          if (originWorker) {
            originWorker.send({
              cmd: 'finalResult',
              result: task.total,
              taskId: msg.taskId,
            })
          }
          delete tasks[msg.taskId]
        }
      }
    })
  }
 
  cluster.on('exit', (worker) => {
    console.log(`[MASTER ${process.pid}] Worker ${worker.process.pid} died`)
  })
}
 
// ---------- Worker Process ----------
else {
  const app = express()
  console.log(`[WORKER ${process.pid}] Started`)
 
  const pendingResponses = {} // Store res and n here
 
  app.get('/api/:n', (req, res) => {
    const n = parseInt(req.params.n)
    const taskId = Date.now() + '-' + process.pid
 
    console.log(`[WORKER ${process.pid}] Received API request to sum 1 to ${n}. Task ID: ${taskId}`)
 
    // Store response and n
    pendingResponses[taskId] = { res, n }
 
    // Notify master to start task
    process.send({
      cmd: 'startTask',
      n,
      taskId,
    })
  })
 
  // Receive messages from master
  process.on('message', (msg) => {
    if (msg.cmd === 'compute') {
      const { start, end, taskId } = msg
      console.log(`[WORKER ${process.pid}] Computing sum from ${start} to ${end} (Task ID: ${taskId})`)
 
      let sum = 0
      for (let i = start; i <= end; i++) {
        sum += i
      }
 
      console.log(`[WORKER ${process.pid}] Sending partial result ${sum} for Task ID: ${taskId}`)
      process.send({
        cmd: 'partialResult',
        result: sum,
        taskId,
      })
    }
 
    // Final result from master
    else if (msg.cmd === 'finalResult') {
      console.log(`[WORKER ${process.pid}] Final result received: ${msg.result} for Task ID: ${msg.taskId}`)
      const { res, n } = pendingResponses[msg.taskId] || {}
      if (res) {
        res.send(`Final sum of numbers 1 to ${n} is ${msg.result} (handled by PID ${process.pid})`)
        delete pendingResponses[msg.taskId]
      }
    }
  })
 
  app.listen(port, () => {
    console.log(`[WORKER ${process.pid}] Server running at http://localhost:${port}`)
  })
}
 
// ---------- Utility ----------
function splitRange(n, parts) {
  const size = Math.floor(n / parts)
  const ranges = []
  let start = 1
 
  for (let i = 0; i < parts; i++) {
    const end = i === parts - 1 ? n : start + size - 1
    ranges.push({ start, end })
    start = end + 1
  }
 
  return ranges
}