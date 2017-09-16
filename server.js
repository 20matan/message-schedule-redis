const express = require('express')
const bodyParser = require('body-parser')
const redis = require("redis")

const app = express()
const redisClient = redis.createClient();
const redisKey = 'messages'

redisClient.on('ready', () => {
  console.log('redis is ready')
})
redisClient.on('error', (err) => {
    console.log('error' + err)
});

app.use( bodyParser.json())
app.use(bodyParser.urlencoded({
  extended: true
}))

// receive message and time
app.post('/echoAtTime', (req, res) => {
  const { time, message } = req.body
  redisClient.zadd([redisKey, time, message], (err, response) => {
    if (err) {
      console.log('error was occured', err)
      res.send('error was occured')
    } else {
      res.send('message was added')
    }
  })
})
app.listen(3000, function () {
  // every second chek if there's a message to execute
  setInterval(() => {
    let currTimestamp = Date.now()
    let queryParams = [redisKey, 0, currTimestamp]

    // get the messages with timestamp between 0 and current timestamp
    redisClient.ZRANGEBYSCORE(queryParams, (err, getResponse) => {
      if (err) {
        throw err
      }

      // delete the messages
      redisClient.ZREMRANGEBYSCORE(queryParams, (err, deleteResponse) => {
        if (err) {
          throw err
        }

        // support load balancing: only if the messages were deleted from
        // this machine, then print them
        if (deleteResponse === getResponse.length) {
          // log the messages
          getResponse.forEach(message => {
            console.log(message)
          })
        }
      })
    })
  }, 1000)
})
