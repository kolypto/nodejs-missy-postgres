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

//endregion


//region Query Parts

/** Query interface
 * @interface
 */
var ModelQueryInterface = exports.ModelQueryInterface = function(model){
    /** Model the query belongs to
     * @type {Model}
     */
    this.model = model;

    /** Escaped table name
     * @type {String}
     */
    this.tableName = escapeIdentifier(model.options.table);

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

    ret.values = _.map(this.fieldNames, function(fieldName){
        // DEFAULT value on undefined
        if (_.isUndefined(entity[fieldName]))
            return 'DEFAULT';
        // Feed param
        else
            return addParam(ret.params, entity[fieldName]);
    });

    return ret;
};

/** Get query string
 * @param {Boolean} [returning=false]
 * @returns {String}
 */
ModelInsertQuery.prototype.queryString = function(returning){
    return 'INSERT INTO ' + this.tableName + ' ' +
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

    /** Where conditions
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
    return 'UPDATE ' + this.tableName + ' ' +
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

    /** Where conditions
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
    return 'DELETE FROM ' + this.tableName + ' ' +
        'WHERE ' + this.where +
        (returning? ' RETURNING *' : '') +
        ';';
};

//endregion
