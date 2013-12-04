'use strict';

var Q = require('q'),
    _ = require('lodash'),
    missy = require('missy'),
    pg = require('pg')
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


exports.testSequence = function(test){

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
    ].reduce(Q.when, Q(1))
        .catch(function(e){ test.ok(false, e.stack); })
        .finally(test.done)
        .done();
};



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
            return _.values(defaultDriverTest.tests).reduce(Q.when, Q(1))
        })
        .catch(function(e){ test.ok(false, e.stack); })
        .finally(test.done)
        .done();
};



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
