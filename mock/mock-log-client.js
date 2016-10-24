'use strict'

var Mockumentary = require('mockumentary')

// create log client mock factory with default methods
var MockLogClient = new Mockumentary({
    log: ()=> undefined,
    error: ()=> undefined,
})

/* exports */
module.exports = MockLogClient