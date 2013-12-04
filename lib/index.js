'use strict';

var missy = require('missy')
    ;

exports.PostgresDriver = require('./PostgresDriver').PostgresDriver;
exports.types = require('./types');

missy.registerDriver('postgres', exports.PostgresDriver);
