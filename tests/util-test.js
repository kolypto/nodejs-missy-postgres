'use strict';

var Q = require('q'),
    _ = require('lodash'),
    u = require('../lib/util'),
    missy = require('missy')
    ;

/** Test prepareCriteria()
 */
exports.testPrepareCriteria = function(test){
    var schema = new missy.Schema('memory'),
        model = schema.define('Model', { id: Number })
        ;

    var c, params, where;

    // Empty criteria
    c = new missy.util.MissyCriteria(model, {});
    params = [0];
    where = u.prepareCriteria(undefined, c, params);

    test.equal(where, '');
    test.deepEqual(params, [0]);

    // Full criteria
    c = new missy.util.MissyCriteria(model, {
        a: 1,
        b: { $gt: 2, $gte: 3 },
        c: { $lt: 4, $lte: 5 },
        d: { $ne: 6 },
        e: { $eq: 7 },

        a1: { $in: 8 },
        a2: { $in: [9,9,9] },
        a3: { $nin: 10 },
        a4: { $nin: [11,11,11] },
        f1: { $exists: true },
        f2: { $exists: false }
    });
    params = [0];
    where = u.prepareCriteria(undefined, c, params);

    _([
        '"a" = $2',
        '"b" > $3',
        '"b" >= $4',
        '"c" < $5',
        '"c" <= $6',
        '"d" <> $7',
        '"e" = $8',
        '"a1" = ANY( $9 )',
        '"a2" = ANY( $10 )',
        '"a3" != ALL( $11 )',
        '"a4" != ALL( $12 )',
        '"f1" IS NOT NULL',
        '"f2" IS NULL',
    ]).each(function(expr, i){
            test.equal(expr, where.split(' AND ')[i])
        });

    test.deepEqual(params, [
        0,1,2,3,4,5,6,7,[8],[9,9,9],[10],[11,11,11]
    ]);

    // Qualified criteria
    c = new missy.util.MissyCriteria(model, {
        a: 1,
        b: { $gt: 2, $gte: 3 }
    });
    params = [0];
    where = u.prepareCriteria('t', c, params);

    _([
        '"t"."a" = $2',
        '"t"."b" > $3',
        '"t"."b" >= $4'
    ]).each(function(expr, i){
            test.equal(expr, where.split(' AND ')[i])
        });

    test.deepEqual(params, [
        0,1,2,3
    ]);

    return test.done();
};

/** Test prepareProjection()
 */
exports.testPrepareProjection = function(test){
    return test.done();
};

/** Test prepareSort()
 */
exports.testPrepareSort = function(test){
    return test.done();
};

/** Test prepareUpdate()
 */
exports.testPrepareUpdate = function(test){
    return test.done();
};
