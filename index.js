const express = require('express')
const os = require('os')

const cluster = require('cluster')
const { count } = require('console')
const port = 3000
const cp_count = os.cpus().length

// console.log(cluster.isPrimary)
let taskIdCounter = 0
let tasks= {}
let result = 0
function splitRange(range, numParts) {
    const step = Math.ceil(range / numParts);
    const result = [];
    
    for (let i = 0; i < numParts; i++) {
      const start = i * step;
      const end = Math.min((i + 1) * step - 1, range);
      result.push([start, end]);
    }
  
    return result;
  }


if (cluster.isPrimary){
    console.log(`Primary ${process.pid} is running`)
    console.log(`lenth of os.cpus(): ${os.cpus().length}`)

    for (let i = 0; i < cp_count; i++){
        const worker = cluster.fork()
        worker.on('message', (msg) => {
            if(msg.cmd === 'start task'){
                console.log('start task fired')
                const taskId = msg.taskId
                // taskIdCounter = taskId
                const n = msg.n
                const requesterId = worker.id
                console.log("requesterId",requesterId)
                const ranges = splitRange(msg.n, cp_count)
                console.log(ranges)
                 tasks[taskId] ={
                    id:requesterId,
                    expectedResults:cp_count,
                    received:0,
                    total:0,
            }
            Object.values(cluster.workers).forEach((w,index)=>{
               
               
                w.send({
                    cmd:'compute',
                    taskId,
                    start:ranges[index][0],
                    end:ranges[index][1]
                })

                
               })

               
               
            }
            
            else if(msg.cmd === 'partial result'){
                
                const task = tasks[msg.taskId]
                if (!task) return
                task.received++
                task.total += msg.result
                console.log('partial result',task.total,msg.result)

                if(task.expectedResults === task.received){
          
                    const originWorker = cluster.workers[task.id]
                    // console.log(originWorker)
                    if(originWorker){
                    originWorker.send({
                        cmd:'final result',
                        result:task.total,
                        taskId:msg.taskId
                    })
                }
                delete tasks[msg.taskId]
                }

                 

            }
           
   
        
    })


    

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`)
    })

}
}
else{
    const pendingResponses = {} // taskId -> res (to send result later)
    const app = express()
    console.log(`Worker ${process.pid} started`)

    app.get('/', (req, res) => {
        res.send('Hello World!')
      })

      app.get('/pid', (req, res) => {
        res.send('Hello World!'+` process id ${process.pid}`)
      })

    app.get('/api/:n',(req,res)=>{
        const n = parseInt(req.params.n)
        let count = 0
        const taskId = Date.now() + '-' + process.pid
        pendingResponses[taskId] = res
       
        
        process.send({
            cmd: 'start task',
            n,
            taskId
          })

        //  process.on('message',(msg)=>{
        //     if(msg.cmd === 'final result'){
        //         console.log(pendingResponses[msg.taskId])
        //         const res = pendingResponses[msg.taskId]
        //         res.send(msg)
        //         delete pendingResponses[msg.taskId]
        //     }
        //  }) 

        // for(let i=0;i<=n;i++){
        //     count += i
        // }
        // process.send({result:count})
        // res.send(`final count is ${count} and process id ${process.pid}`)

        // process.on('message',(msg)=>{
        //     console.log('Process Message')
        //     res.send("Process Message")
        // })
    })

        process.on('message',(msg)=>{
            console.log('first')
            if(msg.cmd === 'compute'){
                let ans = 0
             
                let {start,end,taskId} = msg
                for(let i=start ; i<=end;i++){
                    ans += i
                }
                console.log(start,end,ans)
                process.send({
                    cmd:'partial result',
                    result:ans,
                    taskId
                })
            }
    
            if(msg.cmd === 'final result'){
                // console.log(pendingResponses[msg.taskId])
                const res = pendingResponses[msg.taskId]
                console.log('lst',Object.keys(pendingResponses).length)
                res.send(msg)
                delete pendingResponses[msg.taskId]
            }
         
          
        })


 
      
      app.listen(port, () => {
        console.log(`Server is running at http://localhost:${port}`)
      })


}

