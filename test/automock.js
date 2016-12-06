'use strict'

const ImmutableDatabaseMariaSQL = require('../lib/immutable-database-mariasql')
const MockLogClient = require('../mock/mock-log-client')
const Promise = require('bluebird')
const assert = require('chai').assert

const dbHost = process.env.DB_HOST || 'localhost'
const dbName = process.env.DB_NAME || 'test'
const dbPass = process.env.DB_PASS || ''
const dbUser = process.env.DB_USER || 'root'

// use the same params for all connections
const connectionParams = {
    charset: 'utf8',
    db: dbName,
    host: dbHost,
    password: dbPass,
    user: dbUser,
}

describe('immutable-database-mariasql: automock', function () {

    beforeEach(function () {
        ImmutableDatabaseMariaSQL.reset()
    })

    it('should allow setting an automock function', function () {
        // set automock wrapper function
        ImmutableDatabaseMariaSQL.automock(function () {
            return function () {}
        })
        // test automock function
        assert.isFunction(ImmutableDatabaseMariaSQL.automock())
    })

    it('should call automock function when doing a query', function () {
        // set automock wrapper function
        ImmutableDatabaseMariaSQL.automock(function (connection) {
            // override the query function with mock
            connection.query = function () {
                return Promise.resolve('automock called')
            }
        })
        // instantiate new mariasql client which will call automock wrapper
        // function with new instance
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // do query which should return mock
        return db.query('test').then(function (res) {
            assert.strictEqual(res, 'automock called')
        })
    })

})