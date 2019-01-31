const r = require('rethinkdb')

let databasePromise = null

function connectToDatabase() {
  if(!databasePromise) {
    databasePromise = r.connect({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      db: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timeout: process.env.DB_TIMEOUT,
    })
  }
  return databasePromise
}

module.exports = connectToDatabase
