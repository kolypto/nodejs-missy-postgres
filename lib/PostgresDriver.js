'use strict';

var Q = require('q'),
    events = require('events'),
    util = require('util'),
    pg = require('pg'),
    _ = require('lodash'),
    missy = require('missy'),
    types = require('./types'),
    u = require('./util')
    ;

/** PostgreSQL driver for Missy.
 *
 * @param {Function|String} connect
 *      The connecter function, or a string in 'postgres://localhost/test' format.
 * @param {Object?} options
 *      Driver options
 * @param {Object} options.connect
 *      pg.Client.connect() options (see pg docs)
 *
 * @constructor
 * @implements {IMissyDriver}
 * @extends {EventEmitter}
 */
var PostgresDriver = exports.PostgresDriver = function(connect, options){
    options = options || {};

    // Driver initialization shortcut
    if (!_.isFunction(connect)){
        // Default connecter function
        connect = (function(url){
            var client = new pg.Client(url);
            return function(){
                return Q.nmcall(client, 'connect')
                    .thenResolve(client);
            };
        })(connect);
    }

    // Prepare
    this._connect = connect;
    this.schema = undefined;

    this.client = undefined; // no client
    this.connected = false;
};
util.inherits(PostgresDriver, events.EventEmitter);

PostgresDriver.prototype.toString = function(){
    return 'postgres';
};

PostgresDriver.prototype.connect = function(){
    var self = this;
    return this._connect()
        .then(function(client){
            self.client = client;
            self.connected = true;

            self.client.once('end', function(){
                self.emit('disconnect')
            });

            self.emit('connect');
            return client;
        });
};

PostgresDriver.prototype.disconnect = function(){
    return Q.mcall(this.client, 'end');
};

PostgresDriver.prototype.bindSchema = function(schema){
    this.schema = schema;

    // Register data types
//    this.schema.registerType('ObjectID', types.ObjectID);
};

//region Helpers

//endregion

//region Queries

PostgresDriver.prototype.findOne = function(model, criteria, fields, sort, options){
};

PostgresDriver.prototype.find = function(model, criteria, fields, sort, options){
};

PostgresDriver.prototype.count = function(model, criteria, options){
};

PostgresDriver.prototype.insert = function(model, entities, options){
};

PostgresDriver.prototype.update = function(model, entities, options){
};

PostgresDriver.prototype.save = function(model, entities, options){
};

PostgresDriver.prototype.remove = function(model, entities, options){
};

PostgresDriver.prototype.updateQuery = function(model, criteria, update, options){
};

PostgresDriver.prototype.removeQuery = function(model, criteria, options){
};

//endregion
