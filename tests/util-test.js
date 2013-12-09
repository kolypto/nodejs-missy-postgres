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

    test.strictEqual(where, null);
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
        undefined
    ]).each(function(expr, i){
            test.strictEqual(expr, where.split(' AND ')[i])
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
        '"t"."b" >= $4',
        undefined
    ]).each(function(expr, i){
            test.strictEqual(expr, where.split(' AND ')[i])
        });

    test.deepEqual(params, [
        0,1,2,3
    ]);

    return test.done();
};

/** Test prepareProjection()
 */
exports.testPrepareProjection = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array })
        ;

    var p, select;

    // Empty projection
    p = new missy.util.MissyProjection({});
    select = u.prepareProjection(undefined, User, p);
    test.equal(select, '*');

    // Empty projection: qualified
    p = new missy.util.MissyProjection({});
    select = u.prepareProjection('t', User, p);
    test.equal(select, '"t".*');

    // Inclusion
    p = new missy.util.MissyProjection({ id:1, login:1 });
    select = u.prepareProjection(undefined, User, p);
    test.equal(select, '"id", "login"');

    // Inclusion: qualified
    p = new missy.util.MissyProjection({ id:1, login:1 });
    select = u.prepareProjection('t', User, p);
    test.equal(select, '"t"."id", "t"."login"');

    // Exclusion
    p = new missy.util.MissyProjection({ age:0, roles:0 });
    select = u.prepareProjection(undefined, User, p);
    test.equal(select, '"id", "login"');

    // Exclusion: qualified
    p = new missy.util.MissyProjection({ age:0, roles:0 });
    select = u.prepareProjection('t', User, p);
    test.equal(select, '"t"."id", "t"."login"');

    return test.done();
};

/** Test prepareSort()
 */
exports.testPrepareSort = function(test){
    var s, orderby;

    // Empty
    s = new missy.util.MissySort({});
    orderby = u.prepareSort(undefined, s);
    test.strictEqual(orderby, null);

    // Fields
    s = new missy.util.MissySort({ a:1, b: -1, c: 1 });
    orderby = u.prepareSort(undefined, s);
    test.equal(orderby, '"a" ASC, "b" DESC, "c" ASC');

    // Fields, qualified
    s = new missy.util.MissySort({ a:1, b: -1, c: 1 });
    orderby = u.prepareSort('t', s);
    test.equal(orderby, '"t"."a" ASC, "t"."b" DESC, "t"."c" ASC');

    return test.done();
};

/** Test prepareUpdate()
 */
exports.testPrepareUpdate = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array, old_login: String })
        ;

    var up, update, params;

    // Empty
    up = new missy.util.MissyUpdate(User, {});
    params = [];
    update = u.prepareUpdate(up, params);
    test.strictEqual(update, null);
    test.deepEqual(params, []);

    // Fields
    up = new missy.util.MissyUpdate(User, {
        $set: { id: 1, login: 'a', lol: undefined },
        $inc: { age: 3, id: -4 },
        $unset: { old_login: '', roles: '' },
        $setOnInsert: { id: 10 },
        $rename: { login: 'old_login' }
    });
    params = [];
    update = u.prepareUpdate(up, params);

    _([
        '"id"=$1',
        '"login"=$2',
        '"lol"=DEFAULT',
        '"age"=COALESCE("age",0)+$3',
        '"id"=COALESCE("id",0)+$4',
        '"old_login"=DEFAULT',
        '"roles"=DEFAULT',
        '"id"="id"',
        '"old_login"="login"', '"login"=DEFAULT',
        undefined
    ]).each(function(expr, i){
            test.strictEqual(expr, update.split(', ')[i])
        });
    test.deepEqual(params, [1, 'a',3,-4]);

    return test.done();
};

/** Test prepareLimit()
 */
exports.testPrepareLimit = function(test){
    var l, params;

    // Empty
    params = [];
    l = u.prepareLimit(0,0, params);
    test.strictEqual(l, null);
    test.deepEqual(params, []);

    // Limit
    params = [];
    l = u.prepareLimit(1,0, params);
    test.strictEqual(l, 'LIMIT $1');
    test.deepEqual(params, [1]);

    // Offset
    params = [];
    l = u.prepareLimit(0,1, params);
    test.strictEqual(l, 'OFFSET $1');
    test.deepEqual(params, [1]);

    // Limit Offset
    params = [];
    l = u.prepareLimit(1,1, params);
    test.strictEqual(l, 'LIMIT $1 OFFSET $2');
    test.deepEqual(params, [1,1]);

    test.done();
};

/** Test ModelInsertQuery
 */
exports.testModelInsertQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array, old_login: String })
        ;

    var insertQuery = new u.ModelInsertQuery(User),
        q;

    // Empty
    q = insertQuery.entityQuery({});
    test.equal(q.queryString(true), 'INSERT INTO "users" ("id","login","age","roles","old_login") VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT) RETURNING *;');
    test.deepEqual(q.params, []);

    // With values
    q = insertQuery.entityQuery({ id: 1, login: 'ivy', age: 18 });
    test.equal(q.queryString(true), 'INSERT INTO "users" ("id","login","age","roles","old_login") VALUES($1,$2,$3,DEFAULT,DEFAULT) RETURNING *;');
    test.deepEqual(q.params, [1, 'ivy', 18]);

    test.done();
};

/** Test ModelUpdateQuery
 */
exports.testModelUpdateQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: { type: 'number', required: true, def: 0 } })
        ;

    var updateQuery = new u.ModelUpdateQuery(User),
        q, params;

    // Empty
    test.throws(function(){
        updateQuery.entityQuery({});
    }, missy.errors.MissyModelError);

    // Partial
    q = updateQuery.entityQuery({ id: 1 });
    test.equal(q.queryString(true), 'UPDATE "users" SET "login"=$1, "age"=$2 WHERE "users"."id" = $3 RETURNING *;');
    test.deepEqual(q.params, [null, null, 1]);

    // Full
    q = updateQuery.entityQuery({ id: 1, login: 'dizzy' });
    test.equal(q.queryString(true), 'UPDATE "users" SET "login"=$1, "age"=$2 WHERE "users"."id" = $3 RETURNING *;');
    test.deepEqual(q.params, ['dizzy', null, 1]);

    // Custom: multi=true
    q = updateQuery.customQuery(
        new missy.util.MissyUpdate(User, { a:1, b:2 }),
        new missy.util.MissyCriteria(User, { c:3, d:4 }),
        true
    );
    test.equal(q.queryString(false), 'UPDATE "users" SET "a"=$1, "b"=$2 WHERE "users"."c" = $3 AND "users"."d" = $4;');
    test.deepEqual(q.params, [1,2,3,4]);

    // Custom: multi=false
    q = updateQuery.customQuery(
        new missy.util.MissyUpdate(User, { a:1, b:2 }),
        new missy.util.MissyCriteria(User, { c:3, d:4 }),
        false
    );
    test.equal(q.queryString(false), 'UPDATE "users" SET "a"=$1, "b"=$2 WHERE "id" IN ( SELECT "id" FROM "users" WHERE "users"."c" = $3 AND "users"."d" = $4 LIMIT $5 );');
    test.deepEqual(q.params, [1,2,3,4,1]);

    test.done();
};

/** Test ModelDeleteQuery
 */
exports.testModelDeleteQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { type: String, id: Number }, { pk: ['type', 'id'] })
        ;

    var deleteQuery = new u.ModelDeleteQuery(User),
        q;

    // Empty
    test.throws(function(){
        deleteQuery.entityQuery({});
    }, missy.errors.MissyModelError);

    // Full
    q = deleteQuery.entityQuery({ type: 'user', id: 1, login: 'dizzy' });
    test.equal(q.queryString(true), 'DELETE FROM "users" WHERE "users"."type" = $1 AND "users"."id" = $2 RETURNING *;');
    test.deepEqual(q.params, ['user', 1]);

    // Custom, multi=true
    q = deleteQuery.customQuery(
        new missy.util.MissyCriteria(User, { c:1, d:2 }),
        true
    );
    test.equal(q.queryString(false), 'DELETE FROM "users" WHERE "users"."c" = $1 AND "users"."d" = $2;');
    test.deepEqual(q.params, [1,2]);

    // Custom, multi=false
    q = deleteQuery.customQuery(
        new missy.util.MissyCriteria(User, { c:1, d:2 }),
        false
    );
    test.equal(q.queryString(false), 'DELETE FROM "users" WHERE ROW("type", "id") IN ( SELECT "type", "id" FROM "users" WHERE "users"."c" = $1 AND "users"."d" = $2 LIMIT $3 );');
    test.deepEqual(q.params, [1,2,1]);


    test.done();
};

/** Test ModelSelectQuery
 */
exports.testModelSelectQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array })
        ;

    var selectQuery = new u.ModelSelectQuery(User),
        q;

    // select *
    q = selectQuery.customQuery(
        new missy.util.MissyProjection({})
    );
    test.equal(q.queryString(), 'SELECT "users".* FROM "users";');
    test.deepEqual(q.params, []);

    // select fields
    q = selectQuery.customQuery(
        new missy.util.MissyProjection(['id', 'login'])
    );
    test.equal(q.queryString(), 'SELECT "users"."id", "users"."login" FROM "users";');
    test.deepEqual(q.params, []);

    // select fields where
    q = selectQuery.customQuery(
        new missy.util.MissyProjection(['id']),
        new missy.util.MissyCriteria(User, { age: { $gt: 18 } })
    );
    test.equal(q.queryString(), 'SELECT "users"."id" FROM "users" WHERE "users"."age" > $1;');
    test.deepEqual(q.params, [18]);

    // select * order by
    q = selectQuery.customQuery(
        new missy.util.MissyProjection({}),
        undefined,
        new missy.util.MissySort({ age: -1 })
    );
    test.equal(q.queryString(), 'SELECT "users".* FROM "users" ORDER BY "users"."age" DESC;');
    test.deepEqual(q.params, []);

    // select * where order by limit
    q = selectQuery.customQuery(
        new missy.util.MissyProjection({}),
        undefined,
        new missy.util.MissySort({ age: -1 }),
        15
    );
    test.equal(q.queryString(), 'SELECT "users".* FROM "users" ORDER BY "users"."age" DESC LIMIT $1;');
    test.deepEqual(q.params, [15]);

    // select * where limit offset
    q = selectQuery.customQuery(
        new missy.util.MissyProjection({}),
        undefined,
        new missy.util.MissySort({ age: -1 }),
        15,5
    );
    test.equal(q.queryString(), 'SELECT "users".* FROM "users" ORDER BY "users"."age" DESC LIMIT $1 OFFSET $2;');
    test.deepEqual(q.params, [15,5]);

    test.done();
};

/** Test ModelCountQuery
 */
exports.testModelCountQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array })
        ;

    var countQuery = new u.ModelCountQuery(User),
        q;

    // count all
    q = countQuery.customQuery(
        new missy.util.MissyCriteria(User, {})
    );
    test.equal(q.queryString(), 'SELECT COUNT(*) FROM "users";');
    test.deepEqual(q.params, []);

    // count criteria
    q = countQuery.customQuery(
        new missy.util.MissyCriteria(User, { age: { $gte: 18 } })
    );
    test.equal(q.queryString(), 'SELECT COUNT(*) FROM "users" WHERE "users"."age" >= $1;');
    test.deepEqual(q.params, [18]);

    test.done();
};

/** Test ModelMergeQuery
 */
exports.testModelMergeQuery = function(test){
    var schema = new missy.Schema('memory'),
        User = schema.define('User', { id: Number, login: String, age: Number, roles: Array })
        ;

    var mergeQuery = new u.ModelMergeQuery(User),
        q;

    // returning=false
    q = mergeQuery.entityQuery({ id: 1, login: 'test' });
    test.equal(
        q.queryString(),
        'WITH upsert AS ( ' +
            'UPDATE "users" SET "login"=$1, "age"=$2, "roles"=$3 WHERE "users"."id" = $4 RETURNING * ' +
        ') ' +
        'INSERT INTO "users" ("id", "login") ' +
        'SELECT $5, $6 ' +
        'WHERE NOT EXISTS( SELECT 1 FROM upsert WHERE "upsert"."id" = $7 );'
    );
    test.deepEqual(q.params, ['test', null, null, 1, 1, 'test', 1]);

    // returning=true
    q = mergeQuery.entityQuery({ id: 1, login: 'test' });
    test.equal(
        q.queryString(true),
        'WITH upsert AS ( ' +
            'UPDATE "users" SET "login"=$1, "age"=$2, "roles"=$3 WHERE "users"."id" = $4 RETURNING * ' +
        '), insert AS ( ' +
            'INSERT INTO "users" ("id", "login") ' +
            'SELECT $5, $6 WHERE NOT EXISTS( SELECT 1 FROM upsert WHERE "upsert"."id" = $7 ) RETURNING *' +
        ') ' +
        'SELECT * FROM upsert ' +
        'UNION ' +
        'SELECT * FROM insert;'
    );
    test.deepEqual(q.params, ['test', null, null, 1, 1, 'test', 1]);

    // custom query
    q = mergeQuery.customQuery(
        new missy.util.MissyUpdate(User, { age:18 }),
        new missy.util.MissyCriteria(User, { id:1, login:'test' }),
        true
    );
    test.equal(
        q.queryString(),
        'WITH upsert AS ( ' +
            'UPDATE "users" SET "age"=$1 WHERE "users"."id" = $2 AND "users"."login" = $3 RETURNING * ' +
        ') ' +
        'INSERT INTO "users" ("id", "login", "age") ' +
        'SELECT $4, $5, $6 ' +
        'WHERE NOT EXISTS( SELECT 1 FROM upsert WHERE "upsert"."id" = $7 AND "upsert"."login" = $8 );'
    );
    test.deepEqual(q.params, [18, 1, 'test',  1, 'test', 18,  1, 'test']);

    test.done();
};
