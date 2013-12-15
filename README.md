[![Version](https://badge.fury.io/js/missy-postgres.png)](https://npmjs.org/package/missy-postgres)
[![Dependency Status](https://gemnasium.com/kolypto/nodejs-missy-postgres.png)](https://gemnasium.com/kolypto/nodejs-missy-postgres)

Missy PostgreSQL driver
====================

PostgreSQL database driver for [Missy](https://github.com/kolypto/nodejs-missy).






Usage
=====

Creating a Schema
-----------------

Simple form:

```js
var missy = require('missy').loadDriver('postgres')
    ;

var schema = new missy.Schema('postgres://localhost/test');
```

Full form with manual driver initialization:

```js
var missy = require('missy'),
    PostgresDriver = require('missy-postgres').PostgresDriver,
    pg = require('pg')
    ;

// Driver
var driver = new PostgresDriver(function(){ // Custom connecter function
    // A promise for a client
    var client = new pg.Client('postgres://user:pass@host/database');
    return function(){
        return Q.nmcall(client, 'connect')
            .thenResolve(client);
    ); // -> client
});

// Schema
var schema = new missy.Schema(driver);
```






Type Handlers
-------------

The driver redefines the following standard types:

* `'object'`: is always JSON-encoded





Tests
=====

In order to run the tests, you need to define the 'MISSY_POSTGRES` environment variable.
The tests will work on the provided DB and clean-up the created tables afterwards:

```console
$ MISSY_POSTGRES="postgres://user:pass@localhost/test" npm test
```
