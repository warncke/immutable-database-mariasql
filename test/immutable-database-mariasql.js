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

describe('immutable-database-mariasql', function () {

    beforeEach(function () {
        ImmutableDatabaseMariaSQL.reset()
    })

    it('should connect to database', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // attempt simple query
        return db.query('SELECT CURRENT_TIMESTAMP() AS time')
        // check result
        .then(res => {
            // first row should have a time column
            assert.isOk(res[0].time)
            // validate info
            assert.strictEqual(res.info.numRows, '1')
            assert.strictEqual(res.info.affectedRows, '1')
            assert.strictEqual(res.info.insertId, '0')
            // close connection
            db.close()
        })
    })

    it('should log connection', function () {
        // create mock log client
        var mockLogClient = new MockLogClient({
            log: function (type, data) {
                // validate log type
                assert.strictEqual(type, 'dbConnection')
                // validate log data
                assert.strictEqual(data.connectionName, 'test')
                assert.strictEqual(data.connectionNum, 1)
                assert.deepEqual(data.connectionParams, connectionParams)
                assert.match(data.connectionCreateTime, /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d$/)
                assert.match(data.connectionId, /^[0-9A-Z]{32}$/)
                assert.match(data.instanceId, /^[0-9A-Z]{32}$/)
            },
        })
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams, {
            connectionName: 'test',
            connectionNum: 1,
            logClient: mockLogClient,
        })
        // close connection
        db.close()
    })

    it('should log query and response', function () {
        // capture data
        var connectionId
        var dbQueryId
        // create mock log client
        var mockLogClient = new MockLogClient({
            log: ()=> [
                // 1) log connection
                (type, data) => {
                    // capture connection id
                    connectionId = data.connectionId
                },
                // 2) log query
                (type, data) => {
                    // capture query id
                    dbQueryId = data.dbQueryId
                    // validate log type
                    assert.strictEqual(type, 'dbQuery')
                    // validate data
                    assert.strictEqual(data.connectionId, connectionId)
                    assert.strictEqual(data.moduleCallId, 'Foo')
                    assert.deepEqual(data.options, {foo: 'bar'})
                    assert.deepEqual(data.params, {foo: 'bar'})
                    assert.strictEqual(data.query, 'SELECT CURRENT_TIMESTAMP() AS time')
                    assert.strictEqual(data.requestId, 'Bar')
                },
                // 3) log response
                (type, data) => {
                    // validate log type
                    assert.strictEqual(type, 'dbResponse')
                    // validate data
                    assert.isOk(data.data[0].time)
                    assert.strictEqual(data.dbQueryId, dbQueryId)
                    assert.match(data.dbResponseCreateTime, /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d$/)
                    assert.strictEqual(data.dbResponseSuccess, true)
                    assert.strictEqual(data.info.numRows, '1')
                    assert.strictEqual(data.info.affectedRows, '1')
                    assert.strictEqual(data.info.insertId, '0')
                },
            ],
        })
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams, {
            connectionName: 'test',
            connectionNum: 1,
            logClient: mockLogClient,
        })
        // perform query which should be logged
        return db.query(
            'SELECT CURRENT_TIMESTAMP() AS time',
            // params
            {foo: 'bar'},
            // options (not used, but logged)
            {foo: 'bar'},
            // dummy session data
            {
                moduleCallId: 'Foo',
                requestId: 'Bar',
            }
        )
        // close connection
        .then(() => {
            db.close()
        })
    })

    it('should log query error', function () {
        // capture data
        var dbQueryId
        // create mock log client
        var mockLogClient = new MockLogClient({
            log: ()=> [
                // 1) log connection
                () => {},
                // 2) log query
                (type, data) => {
                    // capture query id
                    dbQueryId = data.dbQueryId
                },
                // 3) log response
                (type, data) => {
                    // validate log type
                    assert.strictEqual(type, 'dbResponse')
                    // validate data
                    assert.match(data.data.message, /You have an error in your SQL syntax/)
                    assert.strictEqual(data.data.code, 1064)
                    assert.strictEqual(data.data.isOperational, true)
                    assert.strictEqual(data.dbQueryId, dbQueryId)
                    assert.match(data.dbResponseCreateTime, /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d$/)
                    assert.strictEqual(data.dbResponseSuccess, false)
                },
            ],
        })
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams, {
            connectionName: 'test',
            connectionNum: 1,
            logClient: mockLogClient,
        })
        // perform invalid query that should log error
        return db.query('SELECT Foobar!')
        // catch error
        .catch(err => {
            assert.match(err.message, /You have an error in your SQL syntax/)
        })
        // close connection
        .then(() => {
            db.close()
        })
    })

    it('should convert nulls to undefined in response data', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // override client query method to return mock data
        db.client.queryPromise = function () {
            return Promise.resolve([
                {
                    foo: null,
                    bar: true,
                },
                {
                    foo: null,
                    bar: true,
                },
            ])
        }
        // perform invalid query - only mock function should be called
        return db.query('SELECT Foobar!')
        // validate response data
        .then(res => {
            assert.deepEqual(res, [ { foo: undefined, bar: true }, { foo: undefined, bar: true } ])
        })
        // close connection
        .then(() => {
            db.close()
        })
    })

    it('should throw error on invalid query arg', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // query should throw on non string for query
        assert.throws(function () { db.query(null) }, Error)
        assert.throws(function () { db.query(false) }, Error)
        assert.throws(function () { db.query(0) }, Error)
        assert.throws(function () { db.query() }, Error)
        // close connection
        db.close()
    })

    it('should throw error on invalid params arg', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // query should throw on non object params
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', null) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', false) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', true) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', []) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', 0) }, Error)
        // try valid args
        assert.doesNotThrow(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}) }, Error)
        // close connection
        db.close()
    })

    it('should throw error on invalid options arg', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // query should throw on non object options
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, null) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, false) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, true) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, []) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, 0) }, Error)
        // try valid args
        assert.doesNotThrow(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}) }, Error)
        // close connection
        db.close()
    })

    it('should throw error on invalid session arg', function () {
        // create new connection
        var db = new ImmutableDatabaseMariaSQL(connectionParams)
        // query should throw on non object options
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, null) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, false) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, true) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, []) }, Error)
        assert.throws(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, 0) }, Error)
        // try valid args
        assert.doesNotThrow(function () { db.query('SELECT CURRENT_TIMESTAMP()', {}, {}, {}) }, Error)
        // close connection
        db.close()
    })

})