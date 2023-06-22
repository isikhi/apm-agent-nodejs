/*
 * Copyright Elasticsearch B.V. and other contributors where applicable.
 * Licensed under the BSD 2-Clause License; you may not use this file except in
 * compliance with the BSD 2-Clause License.
 */

'use strict'

const semver = require('semver')

const { getDBDestination } = require('../context')
const shimmer = require('../shimmer')
const { URL } = require('url')

// Match expected `<hostname>:<port>`, e.g. "mongo:27017", "::1:27017",
// "127.0.0.1:27017".
const HOSTNAME_PORT_RE = /^(.+):(\d+)$/

// Methods that execute a command and returns a promise
// TODO: work with curson methods (findOne)
const COLLECTION_METHODS = [
  'findOne', 'insertOne', 'insertMany',
  'updateOne', 'updateMany', 'replaceOne',
  'deleteOne', 'deleteMany',
  'bulkWrite'
]

// TODO: these are sequences of next calls, do we want to measure it?
// do we want to measure then this and internal next calls?
// - transaction
//   - `toArray` span
//     - `next` span
//     - `next` span
//     - ....
const CURSOR_METHODS = ['toArray', 'forEach']

// Helper to avoid writing
// if (obj && obj.prop && obj.prop.sub_prop && obj.prop.sub_prop.sub_sub_prop)
function _get (obj, path) {
  if (!obj || !path) {
    return obj
  }

  const parts = path.split('.')
  const key = parts.shift()

  return _get(obj[key], parts.join('.'))
}

module.exports = (mongodb, agent, { version, enabled }) => {
  if (!enabled) return mongodb
  if (!semver.satisfies(version, '>=3.3 <6.0')) {
    agent.logger.debug('mongodb version %s not instrumented (mongodb <3.3 is instrumented via mongodb-core)', version)
    return mongodb
  }

  const ins = agent._instrumentation

  const activeSpans = new Map()
  if (mongodb.instrument) {
    const listener = mongodb.instrument()
    listener.on('started', onStart)
    listener.on('succeeded', onEnd)
    listener.on('failed', onEnd)
  } else if (mongodb.MongoClient) {
    // mongodb 4.0+ removed the instrument() method in favor of
    // listeners on the instantiated client objects. There are two mechanisms
    // to get a client:
    // 1. const client = new mongodb.MongoClient(...)
    // 2. const client = await MongoClient.connect(...)
    // both return a MongoClient instance which has a `db` method to get the database
    // and database has `collection` method to get collections of documents
    // we need to instrument the collecitons API

    shimmer.wrap(mongodb.MongoClient.prototype, 'db', wrapDb)
  } else {
    agent.logger.warn('could not instrument mongodb@%s', version)
  }
  return mongodb

  // Agent is interested in instrument the API of collections so it needs to
  // shim the `collection` function to instrument any collection created
  function wrapDb (origDb) {
    return function wrappedDb (dbName, options) {
      const db = origDb.call(this, dbName, options)

      shimmer.wrap(db, 'collection', wrapCollection)
      return db
    }
  }

  // Agent gets the instance of the collection and shims its API
  function wrapCollection (origCollection) {
    return function wrappedCollection (name, options) {
      const coll = origCollection.call(this, name, options)

      for (const methodName of COLLECTION_METHODS) {
        if (typeof coll[methodName] === 'function') {
          shimmer.wrap(coll, methodName, wrapCollectionApi)
        }
      }
      shimmer.wrap(coll, 'find', wrapFind)
      return coll
    }
  }

  function wrapFind (origFind) {
    return function wrappedFind () {
      const cursor = origFind.apply(this, arguments)

      shimmer.wrap(cursor, 'next', wrapNext)
      return cursor
    }
  }

  function wrapNext (origNext) {
    return async function wrappedNext () {
      const result = await origNext.apply(this, arguments)
      console.log('next called')
      return result
    }
  }

  // Since v4.2 methods return a promise with the result if no callback function is passed
  // as last argument
  function wrapCollectionApi (origApi, apiName) {
    return async function wrappedCollectionApi () {
      const { db, namespace } = this.s
      const name = `${namespace.db}.${namespace.collection}.${apiName}`
      const ins = agent._instrumentation
      const span = ins.createSpan(name, 'db', 'mongodb', apiName, { exitSpan: true })

      if (!span) {
        return origApi.apply(this, arguments)
      }

      span.setDbContext({ type: 'mongodb', instance: namespace.db })
      const address = _get(db, 's.client.s.url')
      let destination

      if (address) {
        try {
          const url = new URL(address)
          destination = getDBDestination(url.hostname, url.port)
        } catch {}
      }

      if (destination) {
        span._setDestinationContext(destination)
      } else {
        agent.logger.trace('could not set destination context on mongodb span from address=%j', address)
      }

      const cb = arguments[arguments.length - 1]
      if (typeof cb === 'function') {
        arguments[arguments.length - 1] = function () {
          span.end()
          return cb.apply(null, arguments)
        }
        return origApi.apply(this, arguments)
      }

      try {
        const res = await origApi.apply(this, arguments)
        span.end()
        return res
      } catch (err) {
        span.end()
        throw err
      }
    }
  }

  function onStart (event) {
    // `event` is a `CommandStartedEvent`
    // https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst#api
    // E.g. with mongodb@3.6.3:
    //   CommandStartedEvent {
    //     address: '127.0.0.1:27017',
    //     connectionId: 1,
    //     requestId: 1,
    //     databaseName: 'test',
    //     commandName: 'insert',
    //     command:
    //     { ... } }

    const name = [
      event.databaseName,
      collectionFor(event),
      event.commandName
    ].join('.')

    const span = ins.createSpan(name, 'db', 'mongodb', event.commandName, { exitSpan: true })
    if (span) {
      activeSpans.set(event.requestId, span)

      // Destination context.
      // Per the following code it looks like "<hostname>:<port>" should be
      // available via the `address` or `connectionId` field.
      // https://github.com/mongodb/node-mongodb-native/blob/dd356f0ede/lib/core/connection/apm.js#L155-L169
      const address = event.address || event.connectionId

      console.log(event)
      let match
      if (address && typeof (address) === 'string' &&
          (match = HOSTNAME_PORT_RE.exec(address))) {
        span._setDestinationContext(getDBDestination(match[1], match[2]))
      } else {
        agent.logger.trace('could not set destination context on mongodb span from address=%j', address)
      }

      const dbContext = { type: 'mongodb', instance: event.databaseName }
      span.setDbContext(dbContext)
    }
  }

  function onEnd (event) {
    if (!activeSpans.has(event.requestId)) return
    const span = activeSpans.get(event.requestId)
    activeSpans.delete(event.requestId)
    span.end((span._timer.start / 1000) + event.duration)
  }

  function collectionFor (event) {
    const collection = event.command[event.commandName]
    return typeof collection === 'string' ? collection : '$cmd'
  }
}
