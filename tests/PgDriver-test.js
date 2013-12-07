'use strict';

var Q = require('q'),
    _ = require('lodash'),
    missy = require('missy'),
    pg = require('pg'),
    u = require('../lib/util')
    ;
require('../');

/** Set up the Schema
 */
exports.setUp = function(callback){
    if (!process.env['MISSY_POSTGRES'])
        throw new Error('Environment variable is not set: MISSY_POSTGRES');

    var schema = this.schema = new missy.Schema([
        process.env['MISSY_POSTGRES']
    ]);

    schema.connect()
        .nodeify(callback);
};



/** Test driver: specific cases
 * @param {test|assert} test
 */
exports.testPostgresDriver = function(test){

    var schema = this.schema;

    var Post = schema.define('Post', {
        id: Number,
        title: String,
        length: Number,
        date: Date,
        tags: Array,
        data: Object
    }, { pk: 'id', table: '__test_posts' });

    var now = new Date();

    return [
        // Create schema
        function(){
            return Q.nmcall(schema.getClient(), 'query',
                'CREATE TABLE ' + u.escapeIdentifier(Post.options.table) + ' (' +
                    [
                        '"id" SERIAL',
                        '"title" varchar',
                        '"length" int',
                        '"date" timestamptz',
                        '"tags" varchar[]',
                        '"data" text',
//                        '"data" json',
                        'PRIMARY KEY("id")'
                    ].join(',') +
                ');'
            );
        },
        // Insert test
        function(){
            return Post.insert([
                { title: 'first' },
                { title: 'second', length: 10, tags: ['a','b','c'], data: {a:1,b:2,c:3} },
                { title: 'third', date: now }
            ]).then(function(entities){
                    test.equal(entities.length, 3);
                    test.deepEqual(entities[0], { id: 1, title: 'first', length: null, date: null, tags: null, data: null });
                    test.deepEqual(entities[1], { id: 2, title: 'second', length: 10, date: null, tags: ['a','b','c'], data: {a:1, b:2, c:3} });
                });
        },
        // Insert duplicate key
        function(){
            return Post.insert({ id: 1 })
                .catch(function(e){
                    test.ok(e instanceof missy.errors.EntityExists, e.stack);
                });
        }
    ].reduce(Q.when, Q(1))
        .catch(function(e){ test.ok(false, e.stack); })
        .finally(test.done)
        .done();
};



/** Test driver: common behaviors
 * @param {test|assert} test
 */
exports.testCommonDriverTest = function(test){
    var schema = this.schema;

    var defaultDriverTest = require('../node_modules/missy/tests/driver-common.js').commonDriverTest(test, schema);

    return Q()
        // Create table
        .then(function(){
            return Q.nmcall(schema.getClient(), 'query', [
                'CREATE TABLE "'+defaultDriverTest.User.options.table+'" (',
                '"_id" int NULL,',
                '"login" varchar NULL,',
                '"roles" varchar[] NULL,',
                '"age" int NULL,',
                'PRIMARY KEY ("_id")',
                ');'
            ].join("\n"));
        })
        // Run tests
        .then(function(){
            // NOTE: this driver has a schema and uses `null`s for missing fields. Drop them on load.
            defaultDriverTest.User.hooks.afterImport = function(entity){
                _.each(entity, function(val, fieldName){
                    if (_.isNull(val))
                        delete entity[fieldName];
                });
            };
            // Run
            return _.values(defaultDriverTest.tests).reduce(Q.when, Q(1))
        })
        .catch(function(e){ test.ok(false, e.stack); })
        .finally(test.done)
        .done();
};



/** Tear down the schema
 */
exports.tearDown = function(callback){
    if (!this.schema)
        return callback();

    var schema = this.schema,
        client = schema.getClient();

    // Collect models' tables
    var modelTables = _.map(schema.models, function(model){
        return model.options.table;
    });

    Q()
        // List DB tables
        .then(function(){
            return Q.nmcall(client, 'query', "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public' ;")
                .then(function(result){
                    return _.pluck(result.rows, 'table_name');
                });
        })
        // Remove tables
        .then(function(tables){
            var dropTables = _.intersection(tables, modelTables);
            return Q.all(
                _.map(dropTables, function(table){
                    return Q.nmcall(client, 'query', 'DROP TABLE IF EXISTS "'+table+'";');
                })
            );
        })
        // Disconnect
        .then(function(){
            return schema.disconnect();
        })
        // Finish
        .nodeify(callback);
};
