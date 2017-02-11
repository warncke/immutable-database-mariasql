'use strict'

/* npm modules */
const MariaSQL = require('mariasql')
const Promise = require('bluebird')
const _ = require('lodash')
const instanceId = require('immutable-instance-id')
const microTimestamp = require('micro-timestamp')
const randomUniqueId = require('random-unique-id')
const requireValidLogClient = require('immutable-require-valid-log-client')
const requireValidOptionalObject = require('immutable-require-valid-optional-object')

/* exports */
module.exports = ImmutableDatabaseMariaSQL

/* global variables */

// get reference to global singleton instance
var immutableDatabaseMariasql
// initialize global singleton instance if not yet defined
if (!global.__immutable_database_mariasql__) {
    immutableDatabaseMariasql = global.__immutable_database_mariasql__ = {
        // if set this will be called when instantiating a new
        // db connection to apply the automock wrapper to the
        // connection instance
        automockFunction: undefined,
    }
}
// use existing singleton instance
else {
    immutableDatabaseMariasql = global.__immutable_database_mariasql__
}

/**
 * @function ImmutableDatabaseMariaSQL
 *
 * instantiate a new immutable database connection object using the mariasql
 * driver.
 *
 * @param {object} connectionParams - connection params to pass to db driver
 * @param {object} options - connection options
 *
 * @returns {ImmutableDatabaseMariaSQL}
 *
 * @throws {Error}
 */
function ImmutableDatabaseMariaSQL (connectionParams, options) {
    // validate optional args - create objects if not passed in
    options = requireValidOptionalObject(options)
    // get log client from options
    if (options.logClient !== undefined) {
        // validate that log client has correct interface
        requireValidLogClient(options.logClient)
        // store log client locally until after connection details are logged
        var logClient = options.logClient
    }
    // get unique id
    var uniqueId = randomUniqueId()
    // store connection params
    this.connectionName = options.connectionName
    this.connectionNum = options.connectionNum || 0
    this.connectionParams = connectionParams
    this.connectionCreateTime = uniqueId.timestamp
    this.connectionId = uniqueId.id
    this.instanceId = instanceId.id
    // log the db connection
    if (logClient) {
        // log db connection
        logClient.log('dbConnection', this)
        // store log client for query logging
        this.logClient = logClient
    }
    // create new client instance
    this.client = new MariaSQL(connectionParams)
    // promisify query methods
    this.client.queryPromise = Promise.promisify(this.client.query)
    // set error handler
    this.client.on('error', err => {
        // if log client is set then use it to log error
        if (logClient) {
            logClient.error(err)
        }
        // otherwise console log
        else {
            console.error(err)
        }
    })
    // if automock wrapper function is set then call function with new instance
    // so that automock wrapper can be applied
    if (immutableDatabaseMariasql.automockFunction) {
        immutableDatabaseMariasql.automockFunction(this)
    }
}

/* public methods */

ImmutableDatabaseMariaSQL.automock = automock
ImmutableDatabaseMariaSQL.reset = reset

ImmutableDatabaseMariaSQL.prototype = {
    close: close,
    logQuery: logQuery,
    logQueryError: logQueryError,
    logQueryResponse: logQueryResponse,
    query: query,
}

/**
 * @function automock
 *
 * get/set the automockFunction
 *
 * @param {function|undefined} setAutomockFunction
 *
 * @returns {ImmutableDatabaseMariaSQL|boolean}
 *
 * @throws {Error}
 */
function automock (setAutomockFunction) {
    // set default if value passed
    if (setAutomockFunction !== undefined) {
        // require function
        if (typeof setAutomockFunction !== 'function') {
            throw new Error('automock error: automock must be function')
        }
        // set global value
        immutableDatabaseMariasql.automockFunction = setAutomockFunction
        // return class
        return ImmutableDatabaseMariaSQL
    }
    // return current value
    return immutableDatabaseMariasql.automockFunction
}

/**
 * @function close
 *
 * close connection
 *
 * @param {boolean} force - do not wait for queries to complete
 */
function close (force) {
    force
        ? this.client.destroy()
        : this.client.end()
}

/**
 * @function logQuery
 *
 * log database query start
 *
 * @param {string} query - query string
 * @param {object} params - query params
 * @param {object} options - options to pass client
 * @param {object} session - session object for logging
 * @param {object} dbQueryId - unique id object
 *
 * @returns {undefined}
 */
function logQuery (query, params, options, session, dbQueryId) {
    // require log client
    if (!this.logClient) {
        return
    }
    // do not log if query options flag is false
    if (options.log === false) {
        return
    }
    // log query
    this.logClient.log('dbQuery', {
        connectionId: this.connectionId,
        dbQueryCreateTime: dbQueryId.timestamp,
        dbQueryId: dbQueryId.id,
        moduleCallId: session.moduleCallId,
        options: options,
        params: params,
        query: query,
        requestId: session.requestId,
    })
}

/**
 * @function logQueryError
 *
 * log database query error
 *
 * @param {object} dbQueryId - unique id object
 * @param {object} err - error object
 *
 * @returns {undefined}
 */
function logQueryError (dbQueryId, options, err) {
    // require log client
    if (!this.logClient) {
        return
    }
    // do not log if query options flag is false
    if (options.log === false) {
        return
    }
    // log error
    this.logClient.log('dbResponse', {
        data: {
            code: err.code,
            isOperational: err.isOperational,
            message: err.message,
        },
        dbQueryId: dbQueryId.id,
        dbResponseSuccess: false,
        dbResponseCreateTime: microTimestamp(),
    })
}

/**
 * @function logQueryResponse
 *
 * log database query response
 *
 * @param {object} dbQueryId - unique id object
 * @param {object} res - response data
 *
 * @returns {undefined}
 */
function logQueryResponse (dbQueryId, options, res) {
    // require log client
    if (!this.logClient) {
        return
    }
    // do not log if query options flag is false
    if (options.log === false) {
        return
    }
    // log response
    this.logClient.log('dbResponse', {
        data: res,
        dbQueryId: dbQueryId.id,
        dbResponseCreateTime: microTimestamp(),
        dbResponseSuccess: true,
        info: res.info,
    })
}

/**
 * @function query
 *
 * @param {string} query - query string
 * @param {object} params - query params
 * @param {object} options - options to pass client
 * @param {object} session - session object for logging
 *
 * @returns {Promise}
 *
 * @throws {Error}
 */
function query (query, params, options, session) {
    // require string for query
    if (typeof query !== 'string') {
        throw new Error('query error: query must be string')
    }
    // validate optional args - create objects if not passed in
    options = requireValidOptionalObject(options)
    params = requireValidOptionalObject(params)
    session = requireValidOptionalObject(session)
    // if the no insert flag is set then do not run insert queries
    if (session.noInsert && query.match(/^\s*INSERT/i)) {
        return Promise.resolve()
    }
    // get unique if for query
    var dbQueryId = randomUniqueId()
    // log query start
    this.logQuery(query, params, options, session, dbQueryId)
    // perform query using promisified interface
    return this.client.queryPromise(query, params, options)
    // success
    .then(res => {
        // perform formatting on response data
        formatResponse(res)
        // log response
        this.logQueryResponse(dbQueryId, options, res)
        // resolve with response data
        return res
    })
    // error
    .catch(err => {
        // log error
        this.logQueryError(dbQueryId, options, err)
        // reject with error
        return Promise.reject(err)
    })
}

/**
 * @function reset
 *
 * clear global singleton data
 *
 * @returns {ImmutableDatabaseMariasql}
 */
function reset () {
    // clear global singleton data
    immutableDatabaseMariasql.automockFunction = undefined
    // return class instance
    return ImmutableDatabaseMariaSQL
}

/* private functions */

/**
 * @function formatResponse
 *
 * perform formatting on db response data
 *
 * @param {array} res - database response data
 *
 * @returns {undefined}
 */
function formatResponse (res) {
    // iterate over rows in response
    _.each(res, row => {
        // iterate over key/value pairs in row object
        _.each(row, (val, key) => {
            // convert nulls to undefined
            if (val === null) {
                row[key] = undefined
            }
        })
    })
}