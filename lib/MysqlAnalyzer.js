(function() {
    'use strict';

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , type          = require('ee-types')
        , QueryContext  = require('related-query-context')
        ;






    module.exports = new Class({



        init: function(connection) {
            this.connection = connection;
        }



        /**
         * analyze all dbs available dbs on the connection
         *
         * @returns {Promise} a promise
         */
        , analyze: function(schemas, callback) {
           
            var config = this.connection.getConfig();


            return Promise.all(schemas.map(function(schemaName) {
                return Promise.all(['listContraints', 'describeTables', 'schemaExists'].map(function(fn) {
                    return this[fn](schemaName);
                }.bind(this))).then(function(results) {
                    return Promise.resolve({
                          databaseName: schemaName
                        , constraints:  results[0]
                        , tables:       results[1]
                        , exists:       results[2]
                    });
                }.bind(this));
            }.bind(this))).then(function(definitions) {
                var dbs = {};

                definitions.forEach(function(db){
                    var database;

                    if (!dbs[db.databaseName]) {
                        dbs[db.databaseName] = {};

                        Object.defineProperty(dbs[db.databaseName], 'getDatabaseName', {
                            value: function(){return db.databaseName;}
                        });
                        Object.defineProperty(dbs[db.databaseName], 'schemaExists', {
                            value: function(){return db.exists;}
                        });
                        Object.defineProperty(dbs[db.databaseName], 'getDatabaseAliasName', {
                            value: function(){return config.alias;}
                        });
                    }
                    database = dbs[db.databaseName];


                    // map tables
                    db.tables.forEach(function(definition){
                        var table;

                        if (!database[definition.table_name]) {
                            database[definition.table_name] = {
                                  name          : definition.table_name
                                , primaryKeys   : []
                                , isMapping     : false
                                , columns       : {}
                            };

                            Object.defineProperty(database[definition.table_name], 'getTableName', {
                                value: function(){return definition.table_name;}
                            });
                            Object.defineProperty(database[definition.table_name], 'getDatabaseName', {
                                value: function(){return db.databaseName;}
                            });
                            Object.defineProperty(database[definition.table_name], 'getDatabaseAliasName', {
                                value: function(){return config.alias;}
                            });
                        }
                        table = database[definition.table_name];

                        table.columns[definition.column_name] = this._mapTypes(definition);
                    }.bind(this));


                    // map constraints
                    Object.keys(db.constraints).forEach(function(tableName){

                        // gather info
                        Object.keys(db.constraints[tableName]).forEach(function(constraintName){
                            var   constraint = db.constraints[tableName][constraintName];

                            constraint.rules.forEach(function(rule){
                                switch (constraint.type) {
                                    case 'primary key':
                                        database[tableName].columns[rule.column_name].isPrimary = true;
                                        database[tableName].primaryKeys.push(rule.column_name);
                                        break;

                                    case 'unique':
                                        database[tableName].columns[rule.column_name].isUnique = true;
                                        break;

                                    case 'foreign key':
                                        database[tableName].columns[rule.column_name].isForeignKey = true;
                                        database[tableName].columns[rule.column_name].referencedTable = rule.referenced_table_name;
                                        database[tableName].columns[rule.column_name].referencedColumn = rule.referenced_column_name;
                                        database[tableName].columns[rule.column_name].referencedModel = database[rule.referenced_table_name];

                                        database[tableName].columns[rule.column_name].referenceUpdateAction = rule.on_update.toLowerCase();
                                        database[tableName].columns[rule.column_name].referenceDeleteAction = rule.on_delete.toLowerCase();

                                        // tell the other side its referenced
                                        database[rule.referenced_table_name].columns[rule.referenced_column_name].belongsTo.push({
                                              targetColumn: rule.column_name
                                            , name: tableName
                                            , model: database[tableName]
                                        });
                                        database[rule.referenced_table_name].columns[rule.referenced_column_name].isReferenced = true;
                                        break;
                                }
                            });
                        }.bind(this));


                        Object.keys(db.constraints[tableName]).forEach(function(constraintName){
                            var   constraint = db.constraints[tableName][constraintName];

                            // check for mapping table
                            // a rule must have two memebers and may be of type primary
                            // or unique. if this rule has fks on both column we got a mapping table
                            if (constraint.rules.length === 2 && (constraint.type === 'primary key' || constraint.type === 'unique')){
                                var columns = constraint.rules.map(function(rule){ return rule.column_name; });

                                // serach for fks on both columns, go through all rules on the table, look for a fk constraint
                                if (Object.keys(db.constraints[tableName]).filter(function(checkContraintName){
                                            var checkConstraint = db.constraints[tableName][checkContraintName];

                                            return checkConstraint.type === 'foreign key' && (checkConstraint.rules.filter(function(checkRule){
                                                return columns.indexOf(checkRule.column_name) >= 0;
                                            })).length === 1;
                                        }).length === 2){

                                    database[tableName].isMapping = true;
                                    database[tableName].mappingColumns = columns;

                                    // set mapping reference on tables
                                    var   modelA = database[tableName].columns[columns[0]].referencedModel
                                        , modelB = database[tableName].columns[columns[1]].referencedModel;

                                    modelA.columns[database[tableName].columns[columns[0]].referencedColumn].mapsTo.push({
                                          model         : modelB
                                        , column        : modelB.columns[database[tableName].columns[columns[1]].referencedColumn]
                                        , name          : modelB.name //pluralize.plural(modelB.name)
                                        , via: {
                                              model     : database[tableName]
                                            , fk        : columns[0]
                                            , otherFk   : columns[1]
                                        }
                                    });

                                    // don't add mappings to myself twice
                                    if (modelB !== modelA) {
                                        modelB.columns[database[tableName].columns[columns[1]].referencedColumn].mapsTo.push({
                                              model         : modelA
                                            , column        : modelA.columns[database[tableName].columns[columns[0]].referencedColumn]
                                            , name          : modelA.name //pluralize.plural(modelA.name)
                                            , via: {
                                                  model     : database[tableName]
                                                , fk        : columns[1]
                                                , otherFk   : columns[0]
                                            }
                                        });
                                    }
                                }
                            }
                        }.bind(this));
                    }.bind(this));
                }.bind(this));
        
                return Promise.resolve(dbs);
            }.bind(this));
        }









        /**
         * list all constraints for a database
         *
         * @param {string} databaseName the name of the database 
         *
         * @returns {Promise} a promise     
         */
        , listContraints: function(databaseName) {

            return Promise.all([`
                  SELECT kcu.table_name, 
                         kcu.column_name, 
                         kcu.referenced_table_name, 
                         kcu.referenced_column_name, 
                         kcu.constraint_name,
                         kcu.constraint_catalog,
                    FROM information_schema.key_column_usage kcu
                   WHERE kcu.constraint_schema = '${databaseName}'
                ORDER BY kcu.table_name, 
                         kcu.column_name
            `, `
                  SELECT table_name, constraint_type, constraint_name
                    FROM information_schema.table_constraints
                   WHERE constraint_schema = '${databaseName}'
                ORDER BY table_name, constraint_name, constraint_type
            `, `
                  SELECT rc.update_rule on_update, rc.constraint_catalog, rc.constraint_name
                    FROM information_schema.referential_constraints rc
                   WHERE rc.constraint_schema = '${databaseName}'
            `, `
                  SELECT rc.delete_rule on_delete, rc.constraint_catalog, rc.constraint_name
                    FROM information_schema.referential_constraints rc
                   WHERE rc.constraint_schema = '${databaseName}'
            `].map((sql) => {
                return this.connection.query(sql);
            })).then((results) => {
                var   constraints = {}
                    , tables = {}
                    , handledConstraints = {};


                // could not subselect the on update and 
                // on delete rulesests because each of the
                // subqueries takes 2+ seconds to execute
                // going to do the join over here

                // create a map for the rules
                let updateMap = {};
                results[2].forEach((rule) => {
                    if (!updateMap[rule.constraint_catalog]) updateMap[rule.constraint_catalog] = {};
                    updateMap[rule.constraint_catalog][rule.constraint_name] = rule.on_update;
                });

                let deleteMap = {};
                results[2].forEach((rule) => {
                    if (!deleteMap[rule.constraint_catalog]) deleteMap[rule.constraint_catalog] = {};
                    deleteMap[rule.constraint_catalog][rule.constraint_name] = rule.on_delete;
                });


                // add the constraints to the rules
                results[0].forEach((constraint) => {
                    if (updateMap[constraint.constraint_catalog] && updateMap[constraint.constraint_catalog][constraint.constraint_name]) {
                        constraint.on_update = updateMap[constraint.constraint_catalog][constraint.constraint_name];
                    }

                    if (deleteMap[constraint.constraint_catalog] && deleteMap[constraint.constraint_catalog][constraint.constraint_name]) {
                        constraint.on_delete = deleteMap[constraint.constraint_catalog][constraint.constraint_name];
                    }
                });




                // join the separate results
                results[0].forEach((constraint) => {
                    if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
                    if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: [], type: 'unknown'};

                    constraints[constraint.table_name][constraint.constraint_name].rules.push(constraint);
                });

                results[1].forEach((constraint) => {
                    if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
                    if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: []};

                    constraints[constraint.table_name][constraint.constraint_name].type = constraint.constraint_type.toLowerCase();
                });


                
                return Promise.resolve(constraints);
            });
        }






        /*
         * translate mysql type definition to standard orm type definition
         *
         * @param <Object> mysql column description
         *
         * @returns <Object> standardized type object
         */
        , _mapTypes: function(mysqlDefinition) {
            var ormType = {};

            // column identifier
            ormType.name = mysqlDefinition.column_name.trim();



            // type conversion
            switch (mysqlDefinition.data_type) {
                case 'int':
                case 'tinyint':
                case 'smallint':
                case 'mediumint':
                case 'bigint':
                    ormType.type            = 'integer';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;

                    if (mysqlDefinition.extra === 'auto_increment') ormType.isAutoIncrementing = true;
                    else if (type.string(mysqlDefinition.column_default)) ormType.defaultValue = parseInt(mysqlDefinition.column_default, 10);

                    if (mysqlDefinition.data_type === 'int') ormType.bitLength = 32;
                    else if (mysqlDefinition.data_type === 'tinyint') ormType.bitLength = 8;
                    else if (mysqlDefinition.data_type === 'smallint') ormType.bitLength = 16;
                    else if (mysqlDefinition.data_type === 'mediumint') ormType.bitLength = 24;
                    else if (mysqlDefinition.data_type === 'bigint') ormType.bitLength = 64;
                    break;

                case 'bit':
                    ormType.type            = 'bit';
                    ormType.jsTypeMapping   = 'arrayBuffer';
                    ormType.variableLength  = false;
                    ormType.bitLength       = mysqlDefinition.numeric_precision;
                    break;

                case 'date':
                    ormType.type            = 'date';
                    ormType.jsTypeMapping   = 'date';
                    ormType.variableLength  = false;
                    break;

                case 'character':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = mysqlDefinition.character_maximum_length;
                    break;

                case 'varchar':
                case 'text':
                case 'tinytext':
                case 'mediumtext':
                case 'longtext':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = true;
                    ormType.maxLength       = mysqlDefinition.character_maximum_length;
                    break;

                case 'numeric':
                case 'decimal':
                case 'double':
                    ormType.type            = 'decimal';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = this._scalarToBits(mysqlDefinition.numeric_precision);
                    break;

                case 'float':
                    ormType.type            = 'float';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;
                    ormType.bitLength       = (parseInt(mysqlDefinition.numeric_precision, 10) < 24 ) ? 32 : 64;
                    break;

                case 'datetime':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'date';
                    break;

                case 'timestamp':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = false;
                    ormType.jsTypeMapping   = 'date';
                    break;

                case 'time':
                    ormType.type            = 'time';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'string';
                    break;
            }



            // is null allowed
            ormType.nullable = mysqlDefinition.is_nullable === 'YES';

            // autoincrementing?
            if (!ormType.isAutoIncrementing) ormType.isAutoIncrementing = false;

            // has a default value?
              if (type.undefined(ormType.defaultValue)) {
                if (type.string(mysqlDefinition.column_default)) ormType.defaultValue = mysqlDefinition.column_default;
                else ormType.defaultValue = null;
            }

            // will be set later
            ormType.isPrimary       = false;
            ormType.isUnique        = false;
            ormType.isReferenced    = false;
            ormType.isForeignKey    = false;

            // the native type, should not be used by the users, differs for every db
            ormType.nativeType = mysqlDefinition.data_type;

            // will be filled later
            ormType.mapsTo          = [];
            ormType.belongsTo       = [];

            return ormType;
        }






        /*
         * compute how many bits (bytes) are required to store a certain scalar value
         */
        , _scalarToBits: function(value) {
            var byteLength = 0;

            value = Array.apply(null, {length: parseInt(value, 10)+1}).join('9');

            while(value/Math.pow(2, ((byteLength+1)*8)) > 1) byteLength++;

            return byteLength*8;
        }







        /**
         * fetches detailed data about all table of a database
         *
         * @returns {Promise} a promise
         */
        , describeTables: function(databaseName) {
            return this.connection.query(new QueryContext({
                sql: `SELECT table_schema, table_name, column_name, column_default, is_nullable, data_type, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_schema = '${databaseName}'`
            }));
        }





        /**
         * list all table object of for a specific database
         *
         * @param {string} databaseName the name of the database to list
         *                 the tables for
         *
         * @returns {Promise} a promise
         */
        , listTables: function(databaseName){
            return this.connection.query('SHOW TABLES in '+databaseName+';');
        }





        /**
         * lists all databases
         *
         * @returns {Promise} a promise
         */
        , listDatabases: function() {
            return this.connection.query('SHOW DATABASES;').then((databases) => {
                databases = (databases || []).filter(function(row){
                    return row.Database !== 'information_schema';
                }).map(function(row){
                    return row.Database;
                })

                return Promise.resolve(databases);
            });
        }






        /**
         * checks if a given schema exists
         *
         * @param <String> schemanem
         */
        , schemaExists: function(schemaName) {
            return this.listDatabases().then((schemas) => {
                return Promise.resolve(!!schemas.filter(schema => schema === schemaName).length);
            });
        }
    });
})();
