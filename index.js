const r = require('rethinkdb')
const ReactiveDao = require("reactive-dao")

const db = require('./lib/db.js')
const RethinkObservableValue = require('./lib/RethinkObservableValue.js')
const RethinkObservableList = require('./lib/RethinkObservableList.js')

function command(service, command, parameters) {
  let conn
  return db().then(connection => {
    conn = connection
    let cmd
    if(parameters) {
      cmd = parameters
      cmd.type = command
    } else {
      cmd = command
    }
    cmd.state = "new"
    cmd.timestamp = new Date()
    return r.table( service + "_commands" ).insert(cmd).run(conn)
  }).then( result => {
    let commandId = result.generated_keys[0]
    return r.table( service + '_commands' ).get(commandId).changes({ includeInitial: true  }).run(conn)
  }).then( changesStream => new Promise( (resolve, reject) => {
    changesStream.each( (err, result) => {
      if(err) {
        changesStream.close();
        reject(err)
        return false
      }
      let val = result.new_val
      if(val.state == "done") {
        resolve(val.result)
        changesStream.close()
        return false
      }
      if(val.state == "failed") {
        reject(val.error)
        changesStream.close()
        return false
      }
    })
  }))
}

class SimpleEndpoint {
  constructor({ get, observable }) {
    this.get = get
    this.observable = observable
  }
  next(fun) {
    return new SimpleEndpoint({
      get: (...args) => this.get(...args).then(fun),
      observable: (...args) => this.observable(...args).next(fun)
    })
  }
}

function promiseMap(promise, fn) {
  if(promise.then) return promise.then(fn)
  return fn(promise)
}

function getValue(requestPromise) {
  return Promise.all([db(), requestPromise]).then(([conn, request]) => request.run(conn)).then(
    result => {
      console.log("RES", result)
      return result
    }
  )
}
function observableValue(requestPromise) {
  return new RethinkObservableValue(requestPromise)
}
function simpleValue(requestCallback) {
  return new SimpleEndpoint({
    get: (...args) => getValue( requestCallback('get', ...args) ),
    observable: (...args) => observableValue(
      promiseMap(requestCallback('observe', ...args), req => req.changes({ includeInitial: true }))
    )
  })
}

function getList(requestPromise) {
  return Promise.all([db(), requestPromise]).then(([conn, request]) => request.run(conn))
}
function observableList(requestPromise, idField, maxLength) {
  return new RethinkObservableList(requestPromise, idField, maxLength)
}
function simpleList(requestCallback, idField, maxLength) {
  return new SimpleEndpoint({
    get: (...args) => getList( requestCallback('get', ...args) ),
    observable: (...args) => observableList(
      promiseMap(requestCallback('observe', ...args), req => req.changes({ includeInitial: true, includeStates: true })),
      idField, maxLength
    )
  })
}


module.exports = {
  
  connectToDatabase: db,
  command,

  getValue,
  observableValue,
  simpleValue,

  getList,
  observableList,
  simpleList,
  SimpleEndpoint

}
