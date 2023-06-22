/*
 * Copyright Elasticsearch B.V. and other contributors where applicable.
 * Licensed under the BSD 2-Clause License; you may not use this file except in
 * compliance with the BSD 2-Clause License.
 */

// A small example showing Elastic APM tracing the 'mongodb' package. TODO
//
// This assumes a MongoDB server running on localhost. You can use:
//    npm run docker:start mongodb
// to start a MongoDB docker container. Then `npm run docker:stop` to stop it.

require('../').start({ // elastic-apm-node
  serviceName: 'example-trace-mongodb-concurrent',
  logUncaughtExceptions: true
})

const http = require('http')
const MongoClient = require('mongodb').MongoClient

const DB_NAME = 'example-trace-mongodb-concurrent'
const url = 'mongodb://localhost:27017'

async function bootstrap () {
  // const client = new MongoClient(url)
  try {
    // await client.connect()
    const client = await MongoClient.connect(url)

    const database = client.db(DB_NAME)
    const catsCollection = database.collection('cats')

    const server = http.createServer(function onRequest (req, res) {
      // console.log('incoming request: %s %s %s', req.method, req.url, req.headers)
      req.resume()
      req.on('end', async function () {
        const pathname = req.url
        let resBody = ''
        if (pathname === '/create') {
          catsCollection.insertOne({ name: 'kitty' })
          resBody = 'Meow'
        } else if (pathname === '/getAll') {
          console.log('API /getAll')
          resBody = JSON.stringify(await catsCollection.find({ name: 'kitty' }).toArray())
        }
        // Then reply to the incoming request.
        res.writeHead(200, {
          server: 'example-trace-http',
          'content-type': 'text/plain',
          'content-length': Buffer.byteLength(resBody)
        })
        res.end(resBody)
      })
    })

    server.listen(3000, function () {
      console.log('linstening on port 3000')
    })
    server.on('close', async function () {
      console.log('closing DB conneciton')
      await client.close()
    })
  } catch (err) {
    console.log('bootstrap error', err)
  }
}

bootstrap()
