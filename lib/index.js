var Sequelize = require('sequelize');
var async = require('async');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var dialects = require('./dialects');
var _ = Sequelize.Utils._;

function AutoSequelize(database, username, password, options) {
  this.sequelize = new Sequelize(database, username, password, options || {});
  this.queryInterface = this.sequelize.getQueryInterface();
  this.tables = {};
  this.foreignKeys = {};
  this.dialect = dialects[this.sequelize.options.dialect];

  this.options = _.extend({
    global: 'Sequelize',
    local: 'sequelize',
    spaces: false,
    indentation: 1,
    directory: './models',
    additional: {},
    freezeTableName: true
  }, options || {});
}

AutoSequelize.prototype.build = function(callback) {
  var self = this;

  function mapTable(table, _callback){
    self.queryInterface.describeTable(table).then(function(fields) {
      self.tables[table] = fields
      _callback();
    }, _callback);
  }

  this.queryInterface.showAllTables().then(function (__tables) {
    if (self.sequelize.options.dialect === 'mssql')
      __tables = _.map(__tables, 'tableName');

    var tables = self.options.tables ? _.intersection(__tables, self.options.tables) : __tables;

    async.each(tables, mapForeignKeys, mapTables)

    function mapTables(err) {
      if (err) console.error(err)

      async.each(tables, mapTable, callback);
    }
  }, callback);

  function mapForeignKeys(table, fn) {
    if (! self.dialect) return fn()

    var sql = self.dialect.getForeignKeysQuery(table, self.sequelize.config.database)

    self.sequelize.query(sql, {
      type: self.sequelize.QueryTypes.SELECT,
      raw: true
    }).then(function (res) {
      _.each(res, assignColumnDetails)
      fn()
    }, fn);

    function assignColumnDetails(ref) {
      // map sqlite's PRAGMA results
      ref = _.mapKeys(ref, function (value, key) {
        switch (key) {
        case 'from':
          return 'source_column';
        case 'to':
          return 'target_column';
        case 'table':
          return 'target_table';
        default:
          return key;
        }
      });

      ref = _.assign({
        source_table: table,
        source_schema: self.sequelize.options.database,
        target_schema: self.sequelize.options.database
      }, ref);

      if (! _.isEmpty(_.trim(ref.source_column)) && ! _.isEmpty(_.trim(ref.target_column)))
        ref.isForeignKey = true

      if (_.isFunction(self.dialect.isPrimaryKey) && self.dialect.isPrimaryKey(ref))
        ref.isPrimaryKey = true

       if (_.isFunction(self.dialect.isSerialKey) && self.dialect.isSerialKey(ref))
         ref.isSerialKey = true

      self.foreignKeys[table] = self.foreignKeys[table] || {};
      self.foreignKeys[table][ref.source_column] = _.assign({}, self.foreignKeys[table][ref.source_column], ref);
    }
  }
}

AutoSequelize.prototype.run = function(callback) {
  var self = this;
  var text = {};
  var tables = [];

  this.build(generateText);

  function generateText(err) {
    if (err) console.error(err)

    async.each(_.keys(self.tables), function(table, _callback){
      var fields = _.keys(self.tables[table])
        , spaces = '';

      for (var x = 0; x < self.options.indentation; ++x) {
        spaces += (self.options.spaces === true ? ' ' : "\t");
      }

      text[table] = "";
      text[table] += "import Sequelize from 'sequelize';\n";
      text[table] += "\n";
      text[table] += "import { ModelBuilder } from 'hc-database/sequelize/modelBuilder.js';\n"
      text[table] += "\n";
      text[table] += "export const " + _.capitalize(table) + " = new ModelBuilder().build('" + _.lowerCase(table) + "', {\n";

      _.each(fields, function(field, i){
        // Find foreign key
        var foreignKey = self.foreignKeys[table] && self.foreignKeys[table][field] ? self.foreignKeys[table][field] : null

        if (_.isObject(foreignKey)) {
          self.tables[table][field].foreignKey = foreignKey
        }

        // column's attributes
        var fieldAttr = _.keys(self.tables[table][field]);
        text[table] += spaces + field + ": {\n";

        // Serial key for postgres...
        var defaultVal = self.tables[table][field].defaultValue;

        // ENUMs for postgres...
        if (self.tables[table][field].type === "USER-DEFINED" && !! self.tables[table][field].special) {
          self.tables[table][field].type = "ENUM(" + self.tables[table][field].special.map(function(f){ return "'" + f + "'"; }).join(',') + ")";
        }

        _.each(fieldAttr, function(attr, x){
          var isSerialKey = self.tables[table][field].foreignKey && _.isFunction(self.dialect.isSerialKey) && self.dialect.isSerialKey(self.tables[table][field].foreignKey)

          // We don't need the special attribute from postgresql describe table..
          if (attr === "special") {
            return true;
          }

          if (attr === "foreignKey") {
            if (isSerialKey) {
              text[table] += spaces + spaces + spaces + "autoIncrement: true";
            }
            else if (foreignKey.isForeignKey) {
              text[table] += spaces + spaces + spaces + "references: {\n";
              text[table] += spaces + spaces + spaces + spaces + "model: \'" + self.tables[table][field][attr].target_table + "\',\n"
              text[table] += spaces + spaces + spaces + spaces + "key: \'" + self.tables[table][field][attr].target_column + "\'\n"
              text[table] += spaces + spaces + spaces + "}"
            } else return true;
          }
          else if (attr === "primaryKey") {
             if (self.tables[table][field][attr] === true && (! _.has(self.tables[table][field], 'foreignKey') || (_.has(self.tables[table][field], 'foreignKey') && !! self.tables[table][field].foreignKey.isPrimaryKey)))
              text[table] += spaces + spaces + "primaryKey: true";
            else return true
          }
          else if (attr === "allowNull") {
            text[table] += spaces + spaces + attr + ": " + self.tables[table][field][attr];
          }
          else if (attr === "defaultValue") {
            if ( self.dialect == 'mssql' &&  defaultVal.toLowerCase() === '(newid())' ) {
              defaultVal = null; // disable adding "default value" attribute for UUID fields if generating for MS SQL
            }  

            var val_text = defaultVal;

            if (isSerialKey) return true

            //mySql Bit fix
            if (self.tables[table][field].type.toLowerCase() === 'bit(1)') {
              val_text = defaultVal === "b'1'" ? 1 : 0;
            }

            if (_.isString(defaultVal)) {
              if (self.tables[table][field].type.toLowerCase().indexOf('date') === 0) {
                if (_.endsWith(defaultVal, '()')) {
                  val_text = "sequelize.fn('" + defaultVal.replace(/\(\)$/, '') + "')"
                }
                else if (_.includes(['current_timestamp', 'current_date', 'current_time', 'localtime', 'localtimestamp'], defaultVal.toLowerCase())) {
                  val_text = "sequelize.literal('" + defaultVal + "')"
                } else {
                  val_text = "'" + val_text + "'"
                }
              } else {
                val_text = "'" + val_text + "'"
              }
            }
            if(defaultVal === null) {
              return true;
            } else {
              text[table] += spaces + attr + ": " + val_text;
            }
          }
          else if (attr === "type" && self.tables[table][field][attr].indexOf('ENUM') === 0) {
            text[table] += spaces + spaces + spaces + attr + ": DataTypes." + self.tables[table][field][attr];
          } else {
            var _attr = (self.tables[table][field][attr] || '').toLowerCase();
            var val = "Sequelize." + self.tables[table][field][attr];
            if (_attr === "tinyint(1)" || _attr === "boolean" || _attr === "bit(1)") {
              val = 'Sequelize.BOOLEAN';
            }
            else if (_attr.match(/^(smallint|mediumint|tinyint|int)/)) {
              var length = _attr.match(/\(\d+\)/);
              val = 'Sequelize.INTEGER' + (!  _.isNull(length) ? length : '');
            }
            else if (_attr.match(/^bigint/)) {
              val = 'Sequelize.BIGINT';
            }
            else if (_attr.match(/^string|varchar|varying|nvarchar/)) {
              val = 'Sequelize.TEXT';
            }
            else if (_attr.match(/^char/)) {
              var length = _attr.match(/\(\d+\)/);
              val = 'Sequelize.CHAR' + (!  _.isNull(length) ? length : '');
            }
            else if (_attr.match(/text|ntext$/)) {
              val = 'Sequelize.TEXT';
            }
            else if (_attr.match(/^(date|time)/)) {
              val = 'Sequelize.DATE';
            }
            else if (_attr.match(/^(float|float4)/)) {
              val = 'Sequelize.FLOAT';
            }
            else if (_attr.match(/^decimal/)) {
              val = 'Sequelize.DECIMAL';
            }
            else if (_attr.match(/^(float8|double precision)/)) {
              val = 'Sequelize.DOUBLE';
            }
            else if (_attr.match(/^uuid|uniqueidentifier/)) {
              val = 'Sequelize.UUIDV4';
            }
            else if (_attr.match(/^json/)) {
              val = 'Sequelize.JSON';
            }
            else if (_attr.match(/^jsonb/)) {
              val = 'Sequelize.JSONB';
            }
            else if (_attr.match(/^geometry/)) {
              val = 'Sequelize.GEOMETRY';
            }
            text[table] += spaces + spaces + attr + ": " + val;
          }

          text[table] += ",";
          text[table] += "\n";
        });

        text[table] += spaces + "}";
        text[table] += ",";
        text[table] += "\n";
      });

      text[table] += "}";

      //conditionally add additional options to tag on to orm objects
      var hasadditional = _.isObject(self.options.additional) && _.keys(self.options.additional).length > 0;

      if (hasadditional) {
        _.each(self.options.additional, addAdditionalOption)
      }

      text[table] = text[table].trim()
      text[table] = text[table].substring(0, text[table].length - 1);
      text[table] += "}";

      function addAdditionalOption(value, key) {
        if (key === 'name') {
          // name: true - preserve table name always
          text[table] += spaces + spaces + "name: {\n";
          text[table] += spaces + spaces + spaces + "singular" + ": '" + table + "',\n";
          text[table] += spaces + spaces + spaces + "plural" + ": '" + table + "'\n";
          text[table] += spaces + spaces + "},\n";
        }
        else {
          text[table] += spaces + spaces + key + ": " + value + ",\n";
        }
      }

      //resume normal output
      text[table] += ");\n";
      _callback(null);
    }, function(){
      self.sequelize.close();
      self.write(text, callback);
    });
  }
}

AutoSequelize.prototype.write = function(attributes, callback) {
  var tables = _.keys(attributes);
  var self = this;

  mkdirp.sync(path.resolve(self.options.directory))

  async.each(tables, createFile, callback)

  function createFile(table, _callback){
    fs.writeFile(path.resolve(path.join(self.options.directory, table + '.js')), attributes[table], _callback);
  }
}

module.exports = AutoSequelize
