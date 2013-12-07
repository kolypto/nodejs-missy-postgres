'use strict';

var Q = require('q'),
    _ = require('lodash'),
    missy = require('missy'),
    pg = require('pg')
    ;

// TODO: remake this to a library with `dialect` parameter

//region Helpers

/** Quote an identifier
 * @param {String} column
 * @returns {String}
 */
var escapeIdentifier = exports.escapeIdentifier = function(column, table){
    return (table? escapeIdentifier(table) + '.' : '') + pg.Client.prototype.escapeIdentifier(column);
};

/** Add a parameter to the array and return its identifier
 * @param {Array} params
 *      Query parameters array
 * @param {*} value
 *      The value to add
 * @returns {String} '$N'
 */
var addParam = exports.addParam = function(params, value){
    params.push(value);
    return '$' + (params.length);
};

//endregion



//region Missy Model Utils

/** Convert MissyCriteria to PostgreSQL WHERE
 * @param {String?} table
 *      Table name to prefix the columns with
 * @param {MissyCriteria} criteria
 *      The criteria
 * @param {Array} params
 *      Query params array
 * @returns {String?}
 */
var prepareCriteria = exports.prepareCriteria = function(table, criteria, params){
    // Prepare
    var where = [],
        opmap = {
            '$gt': '>',
            '$gte': '>=',
            '$lt': '<',
            '$lte': '<=',
            '$ne': '<>',
            '$eq': '='
        };

    // Convert conditions
    _.each(criteria.criteria, function(test, fieldName){
        var f = escapeIdentifier(fieldName, table);
        _.each(test, function(operand, operator){
            switch (operator){
                case '$gt':
                case '$gte':
                case '$lt':
                case '$lte':
                case '$ne':
                case '$eq':
                    where.push([ f, opmap[operator], addParam(params, operand) ].join(' '));
                    break;
                case '$in':
                case '$nin':
                    where.push([
                        f,
                        { '$in': '= ANY(', '$nin': '!= ALL(' }[operator],
                        addParam(params, operand),
                        ')'
                    ].join(' '));
                    break;
                case '$exists':
                    where.push([ f, operand? 'IS NOT NULL' : 'IS NULL' ].join(' '));
                    break;
                default:
                    throw new Error('Unsupported operator: ' + operator);
            }
        })
    });
    return where.length ? where.join(' AND ') : null;
};

/** Convert MissyProjection to PostgreSQL SELECT expressions
 * @param {String?} table
 *      Table name to prefix the columns with
 * @param {Model} model
 *      The model to project
 * @param {MissyProjection} fields
 *      Fields projection
 * @returns {String}
 */
var prepareProjection = exports.prepareProjection = function(table, model, fields){
    // Empty projection
    if (_.isEmpty(fields.projection))
        return (table? escapeIdentifier(table) + '.' : '') + '*';
    // Specified projection
    return _.map(
        fields.getFieldDetails(model).fields,
        function(fieldName){
            return escapeIdentifier(fieldName, table);
        }
    ).join(', ')
};

/** Convert MissySort to PostgreSQL ORDER BY expressions
 * @param {String?} table
 *      Table name to prefix the columns with
 * @param {MissyProjection} sort
 *      Sort specification
 * @returns {String?}
 */
var prepareSort = exports.prepareSort = function(table, sort){
    // Empty sort
    if (_.isEmpty(sort.sort))
        return null;
    // Sort fields
    return _.map(
        sort.sort,
        function(dir, fieldName){
            return escapeIdentifier(fieldName, table) + ' ' + ((dir === +1)? 'ASC' : 'DESC');
        }
    ).join(', ');
};

/** Convert MissyUpdate to PostgreSQL SET assignments
 * @param {String?} table
 *      Table name to prefix the columns with
 * @param {MissyUpdate} update
 *      Update operations
 * @param {Array} params
 *      Query parameters
 * @returns {String?}
 */
var prepareUpdate = exports.prepareUpdate = function(update, params){
    // Empty update
    if (_.isEmpty(update.update))
        return null;
    // Operations
    return _.flatten(_.map(
        update.update,
        function(fields, operator){
            return _.map(fields, function(value, fieldName){
                fieldName = escapeIdentifier(fieldName);
                switch (operator){
                    case '$set':
                        return fieldName + '=' + (_.isUndefined(value) ? 'DEFAULT' : addParam(params, value));
                    case '$inc':
                        return fieldName + '=' + fieldName + '+' + addParam(params, value);
                    case '$unset':
                        return fieldName + '=DEFAULT';
                    case '$setOnInsert':
                        return fieldName + '=' + fieldName;
                    case '$rename':
                        return [
                            escapeIdentifier(value) + '=' + fieldName,
                            fieldName + '=DEFAULT'
                        ];
                    default:
                        throw new Error('Unsupported operator: ' + operator);
                }
            });
        }
    )).join(', ');
};

/** Convert limit-offset pair to PostgreSQL LIMIT clause
 * @param {Number} limit
 *      Rows limit
 * @param {Number} offset
 *      Rows offset
 * @param {Array} params
 *      Query parameters
 * @returns {String?}
 */
var prepareLimit = exports.prepareLimit = function(limit, offset, params){
    if (!limit && !offset)
        return null;
    return (limit? 'LIMIT ' + addParam(params, limit) : '') +
        (limit && offset? ' ' : '') +
        (offset? 'OFFSET ' + addParam(params, offset) : '');
};

/** Convert an entity to comma-separated arguments
 * @param {Array.<String>} fieldNames
 *      Field names to include in order
 * @param {Object} entity
 *      The entity to use
 * @param {Array} params
 *      Query parameters
 * @type {prepareInsertValues}
 */
var prepareInsertValues = exports.prepareInsertValues = function(fieldNames, entity, params){
    return _.map(fieldNames, function(fieldName){
        // DEFAULT value on undefined
        if (_.isUndefined(entity[fieldName]))
            return 'DEFAULT';
        // Feed param
        else
            return addParam(params, entity[fieldName]);
    });
};

//endregion


//region Model Queries

/** Collection of Model Query Interfaces
 * @param {Model} model
 * @constructor
 */
var ModelQueries = exports.ModelQueries = function(model){
    this.select = new ModelSelectQuery(model);
    this.insert = new ModelInsertQuery(model);
    this.update = new ModelUpdateQuery(model);
    this.delete = new ModelDeleteQuery(model);
};



/** Query interface
 * @interface
 */
var ModelQueryInterface = exports.ModelQueryInterface = function(model){
    /** Model the query belongs to
     * @type {Model}
     */
    this.model = model;

    /** Table name
     * @type {String}
     */
    this.tableName = model.options.table;

    /** Escaped table name
     * @type {String}
     */
    this.table = escapeIdentifier(model.options.table);

    /** Field names
     * @type {Array.<String>}
     */
    this.fieldNames = _.keys(model.fields);

    /** Escaped fields
     * @type {Array.<String>}
     */
    this.fields = _.map(this.fieldNames, function(fieldName){
        return escapeIdentifier(fieldName);
    });

    /** Query parameters
     * @type {Array}
     */
    this.params = undefined;
};



/** INSERT query
 * @param {Model} model
 * @constructor
 * @implements {ModelQueryInterface}
 */
var ModelInsertQuery = exports.ModelInsertQuery = function(model){
    ModelQueryInterface.call(this, model);

    /** Insert values
     * @type {Array.<String>}
     */
    this.values = undefined;
};

/** Prepare to insert an entity
 * @param {Object} entity
 * @returns {ModelInsertQuery}
 */
ModelInsertQuery.prototype.entityQuery = function(entity){
    var ret = Object.create(this);

    ret.params = [];
    ret.values = prepareInsertValues(this.fieldNames, entity, ret.params);

    return ret;
};

/** Get query string
 * @param {Boolean} [returning=false]
 * @returns {String}
 */
ModelInsertQuery.prototype.queryString = function(returning){
    return 'INSERT INTO ' + this.table + ' ' +
        '('+ this.fields.join(',') +') ' +
        'VALUES('+ this.values.join(',') +')' +
        (returning? ' RETURNING *' : '') +
        ';';
};



/** UPDATE query
 * @param {Model} model
 * @constructor
 * @implements {ModelQueryInterface}
 */
var ModelUpdateQuery = exports.ModelUpdateQuery = function(model){
    ModelQueryInterface.call(this, model);

    /** Column assignments
     * @type {Array.<String>}
     */
    this.assign = undefined;

    /** WHERE conditions
     * @type {String}
     */
    this.where = undefined;
};

/** Prepare a custom query
 * @param {MissyUpdate} update
 * @param {MissyCriteria} where
 */
ModelUpdateQuery.prototype.customQuery = function(update, where){
    var ret = Object.create(this);

    ret.params = [];
    ret.assign = prepareUpdate(update, ret.params);
    ret.where = prepareCriteria(this.model.options.table, where, ret.params);

    return ret;
};

/** Prepare to update an entity: full update
 * @param {Object} entity
 * @returns {ModelUpdateQuery}
 */
ModelUpdateQuery.prototype.entityQuery = function(entity){
    return this.customQuery(
        // assign to ALL non-PK fields
        new missy.util.MissyUpdate(this.model, {
            $set: _.defaults(
                _.omit(entity, this.model.options.pk),
                _.object( _.difference(this.fieldNames, this.model.options.pk), [] ) // init all missing fields with defaults
            )
        }),
        // locate by PK fields
        missy.util.MissyCriteria.fromEntity( this.model, entity )
    );
};

/** Get query string
 * @param {Boolean} [returning=false]
 * @returns {String}
 */
ModelUpdateQuery.prototype.queryString = function(returning){
    return 'UPDATE ' + this.table + ' ' +
        'SET ' + this.assign + ' ' +
        'WHERE ' + this.where +
        (returning? ' RETURNING *' : '') +
        ';';
};



/** DELETE query
 * @param {Model} model
 * @constructor
 * @implements {ModelQueryInterface}
 */
var ModelDeleteQuery = exports.ModelDeleteQuery = function(model){
    ModelQueryInterface.call(this, model);

    /** WHERE conditions
     * @type {String}
     */
    this.where = undefined;
};

/** Prepare a custom query
 * @param {MissyCriteria} where
 */
ModelDeleteQuery.prototype.customQuery = function(where){
    var ret = Object.create(this);

    ret.params = [];
    ret.where = prepareCriteria(this.model.options.table, where, ret.params);

    return ret;
};

/** Prepare to delete an entity
 * @param {Object} entity
 * @returns {ModelUpdateQuery}
 */
ModelDeleteQuery.prototype.entityQuery = function(entity){
    return this.customQuery(
        // By PK
        missy.util.MissyCriteria.fromEntity( this.model, entity )
    );
};

/** Get query string
 * @param {Boolean} [returning=false]
 * @returns {String}
 */
ModelDeleteQuery.prototype.queryString = function(returning){
    return 'DELETE FROM ' + this.table + ' ' +
        'WHERE ' + this.where +
        (returning? ' RETURNING *' : '') +
        ';';
};



/** SELECT query
 * @param {Model} model
 * @constructor
 * @implements {ModelQueryInterface}
 */
var ModelSelectQuery = exports.ModelSelectQuery = function(model){
    ModelQueryInterface.call(this, model);

    /** SELECT fields
     * @type {String}
     */
    this.select = undefined;

    /** WHERE conditions
     * @type {String?}
     */
    this.where = undefined;

    /** SORT clause
     * @type {String?}
     */
    this.sort = undefined;

    /** LIMIT clause
     * @type {String?}
     */
    this.limit = undefined;
};

/** Prepare a custom query
 * @param {MissyProjection?} select
 * @param {MissyCriteria?} where
 * @param {MissySort?} sort
 * @param {Number} [limit=0]
 * @param {Number} [offset=0]
 */
ModelSelectQuery.prototype.customQuery = function(select, where, sort, limit, offset){
    var ret = Object.create(this);

    ret.params = [];
    ret.select = prepareProjection(this.model.options.table, this.model, select);
    ret.where = where? prepareCriteria(this.model.options.table, where, ret.params) : null;
    ret.sort = sort? prepareSort(this.model.options.table, sort) : null;
    ret.limit = prepareLimit(limit, offset, ret.params);

    return ret;
};

/** Get query string
 * @returns {String}
 */
ModelSelectQuery.prototype.queryString = function(){
    return _.compact([
        'SELECT ' + this.select,
        'FROM ' + this.table,
        (this.where? 'WHERE ' + this.where : ''),
        (this.sort? 'ORDER BY ' + this.sort : ''),
        (this.limit? this.limit : '')
    ]).join(' ') + ';';
};



/** MERGE query
 * @param {Model} model
 * @constructor
 * @implements {ModelQueryInterface}
 */
var ModelMergeQuery = exports.ModelMergeQuery = function(model){
    ModelQueryInterface.call(this, model);

    /** Update query
     * @type {String}
     */
    this.update = undefined;

    /** Insert-select values
     * @type {Array.<String>}
     */
    this.values = undefined;

    /** Insert WHERE conditions
     * @type {String}
     */
    this.condition = undefined;
};

/** Prepare to merge an entity
 * @param {Object} entity
 * @returns {ModelInsertQuery}
 */
ModelMergeQuery.prototype.entityQuery = function(entity){
    var ret = Object.create(this);

    // Update CTE: http://stackoverflow.com/a/8702291/134904
    var updateQuery = new ModelUpdateQuery(this.model),
        q = updateQuery.entityQuery(entity)
        ;
    ret.update = q.queryString(true).replace(/;$/, ''); // no trailing ';'
    ret.params = q.params;

    // Insert
    ret.fields = _.clone(this.fields); // we're going to remove fields!
    ret.values = [];
    _.each(this.fieldNames, function(fieldName, i){
        // DEFAULT value on undefined
        if (_.isUndefined(entity[fieldName]))
            delete ret.fields[i];
        // Feed param
        else
            ret.values.push(addParam(ret.params, entity[fieldName]));
    });
    ret.fields = _.compact(ret.fields);

    // Condition
    ret.condition = prepareCriteria('upsert', missy.util.MissyCriteria.fromEntity(this.model, entity), ret.params);

    return ret;
};

/** Get query string
 * @returns {String}
 */
ModelMergeQuery.prototype.queryString = function(){
    return [
        'WITH upsert AS (', this.update, ')',
        'INSERT INTO ' + this.table, '('+ this.fields.join(', ') +')',
        'SELECT', this.values.join(', '),
        'WHERE NOT EXISTS(',
            'SELECT 1 FROM upsert WHERE', this.condition,
        ');'
    ].join(' ');
};

//endregion
