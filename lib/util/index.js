'use strict';

var Q = require('q'),
    _ = require('lodash'),
    missy = require('missy'),
    pg = require('pg')
    ;

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

//region Query parts

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
                var f = escapeIdentifier(fieldName);
                switch (operator){
                    case '$set':
                        return f + '=' + addParam(params, value);
                    case '$inc':
                        return f + '=' + f + '+' + addParam(params, value);
                    case '$unset':
                        return f + '=DEFAULT';
                    case '$setOnInsert':
                        return f + '=' + f;
                    case '$rename':
                        return [
                            escapeIdentifier(value) + '=' + f,
                            f + '=DEFAULT'
                        ];
                    default:
                        throw new Error('Unsupported operator: ' + operator);
                }
            });
        }
    )).join(', ');
};

//endregion
