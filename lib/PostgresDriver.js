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

    // Field types
    this.schema.registerType('object', missy.types.JSON); // always JSON-encode objects
};

//region Helpers

/** Get ModelQueryInterface objects for a model
 * @param {Model} model
 * @returns {ModelQueries}
 * @protected
 */
PostgresDriver.prototype._getModelQueries = function(model){
    if (_.isUndefined(this._modelQueries))
        this._modelQueries = {};

    // TODO: optimize query interfaces with reusable prepared statements

    if (!this._modelQueries[model.name])
        this._modelQueries[model.name] = new u.ModelQueries(model);

    return this._modelQueries[model.name];
};

/** Wrap an error into MissyDriverError
 * @param {Error} e
 * @throws {MissyDriverError}
 * @protected
 */
PostgresDriver.prototype._wrapError = function(e){
    throw new missy.errors.MissyDriverError(this, e.message + ': ' + e.detail);
};

//endregion

//region Queries

PostgresDriver.prototype.findOne = function(model, criteria, fields, sort, options){
    return this.find(model,
        criteria, fields, sort,
        _.extend(options, { limit: 1, skip: 0 })
    ).then(function(entities){
        return entities[0] || null;
    });
};

PostgresDriver.prototype.find = function(model, criteria, fields, sort, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q = modelQuery.select.customQuery(fields, criteria, sort, options.limit, options.skip);
    return Q.nmcall(self.client, 'query', q.queryString(), q.params)
        .get('rows')
        .catch(self._wrapError.bind(self));
};

PostgresDriver.prototype.count = function(model, criteria, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q = modelQuery.count.customQuery(criteria);
    return Q.nmcall(self.client, 'query', q.queryString(), q.params)
        .get('rows')
        .then(function(rows){
            return +rows[0].count; // Number cast
        })
        .catch(self._wrapError.bind(self));
};

PostgresDriver.prototype.insert = function(model, entities, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q;

    return Q.all(_.map(entities, function(entity){
        q = modelQuery.insert.entityQuery(entity);
        return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
            .get('rows').get(0)
            .catch(function(e){
                if (e.code === '23505')
                    throw new missy.errors.EntityExists(model, entity);
                else
                    self._wrapError(e);
            });
    }));
};

PostgresDriver.prototype.update = function(model, entities, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q;

    return Q.all(_.map(entities, function(entity){
        q = modelQuery.update.entityQuery(entity);
        return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
            .catch(self._wrapError.bind(self))
            .get('rows').get(0)
            .then(function(entity){
                if (_.isUndefined(entity))
                    throw new missy.errors.EntityNotFound(model, entity);
                return entity;
            });
    }));
};

PostgresDriver.prototype.save = function(model, entities, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q;

    return Q.all(_.map(entities, function(entity){
        q = modelQuery.merge.entityQuery(entity);
        return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
            .get('rows').get(0)
            .catch(self._wrapError.bind(self));
    }));
};

PostgresDriver.prototype.remove = function(model, entities, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q;

    return Q.all(_.map(entities, function(entity){
        q = modelQuery.delete.entityQuery(entity);
        return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
            .get('rows').get(0)
            .catch(self._wrapError.bind(self))
            .then(function(entity){
                if (_.isUndefined(entity))
                    throw new missy.errors.EntityNotFound(model, entity);
                return entity;
            });
    }));
};

PostgresDriver.prototype.updateQuery = function(model, criteria, update, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q = modelQuery[ options.upsert? 'merge' : 'update' ].customQuery(update, criteria, options.multi);
    return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
        .get('rows')
        .catch(self._wrapError.bind(self));
};

PostgresDriver.prototype.removeQuery = function(model, criteria, options){
    var self = this,
        modelQuery = this._getModelQueries(model),
        q = modelQuery.delete.customQuery(criteria, options.multi);
    return Q.nmcall(self.client, 'query', q.queryString(true), q.params)
        .get('rows')
        .catch(self._wrapError.bind(self));
};

//endregion
