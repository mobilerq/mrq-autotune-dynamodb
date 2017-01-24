const _ = require('lodash');
const Promise = require('bluebird')
const Slack = require('node-slack');
const Intl = require('intl');

const AWS = require('aws-sdk');
AWS.config.setPromisesDependency(Promise);
AWS.config.maxRetries = 0;
//AWS.config.logger = console;

const dynamodb = new AWS.DynamoDB();
const cw = new AWS.CloudWatch();

const COLD_THRESHOLD_RATIO = 0.5;
const HOT_THRESHOLD_RATIO = 0.95;
const COOLDOWN_PERIOD_MINUTES = 120;
const WARMUP_PERIOD_MINUTES = 1;
const INCREASE_TO_DECREASE_MIN_MINUTES = 30;
const MIN_CAPACITY = 1;
const MAX_CAPACITY = 11000;
const CONSUMPTION_STATISTICAL_PERIOD_MINUTES = 15;

// Send to slack if SLACK_HOOK_URL environment is set
const SLACK_HOOK_URL = process.env.SLACK_HOOK_URL;

// this defines when an increase is made, and to what value.
// this function is applied for read and write capacities in separate calls. 
function getIncrease(
  createdMinutesAgo,
  lastIncreaseMinutesAgo,
  lastDecreaseMinutesAgo,
  currentCapacity,
  currentConsumption,
  capacityConsumptionRatio
) {
  if (lastIncreaseMinutesAgo > WARMUP_PERIOD_MINUTES &&
    capacityConsumptionRatio > HOT_THRESHOLD_RATIO) {
    // increase capacity to 120% of current consumption or capacity
    return Math.max(currentCapacity, currentConsumption) * 1.2;
  }
}

// this defines when a decrease is made, and to what value.
// this function is applied for read and write capacities in separate calls. 
function getDecrease(
  createdMinutesAgo,
  lastIncreaseMinutesAgo,
  lastDecreaseMinutesAgo,
  numberDecreasesRemainingToday,
  currentCapacity,
  currentConsumption,
  capacityConsumptionRatio
) {
  if (lastIncreaseMinutesAgo > INCREASE_TO_DECREASE_MIN_MINUTES && 
    lastDecreaseMinutesAgo > COOLDOWN_PERIOD_MINUTES &&
    createdMinutesAgo > COOLDOWN_PERIOD_MINUTES &&
    capacityConsumptionRatio < COLD_THRESHOLD_RATIO &&
    numberDecreasesRemainingToday > 0) {
    // decrease capacity to 120% of current consumption
    return currentConsumption * 1.2;
  }
}

// this is the AWS Lambda handler.
// this function defines the high-level sequence of
// asynchronous API calls and data processing steps.
exports.handler = (event, context, callback) => {
  getTableNames()
    .tap(tableNames => {
      log('Number of tables: ' + tableNames.length);
      if (_.uniq(tableNames).length != tableNames.length) throw "Got duplicate table names!";
    })
    // for each table name, load table description and CloudWatch consumption data.
    // each table is represented as an array containing 1 table record and any number indexes.
    .then(tableNames => Promise.all(tableNames.map(table)))
    // each table and its indexes must all be ACTIVE.
    // filter out those that are not.
    .then(tables => _.filter(tables, tables1 => _.every(tables1, table => table.Status == 'ACTIVE')))
    .tap(tables => log('Number of active tables: ' + tables.length))
    // flatten the table/indexes collections.
    .then(tables => _.flatten(tables))
    .tap(tables => log('Number of tables and indexes: ' + tables.length))
    // calculate all adjustment values .
    .then(tables => tables.map(getAdjustments))
    // filter out records that are not adjusted.
    .then(tables => tables.filter(table => table.WriteAdjustment || table.ReadAdjustment))
    .tap(tables => log('Number of adjustments: ' + tables.length))
    .then(tables => Promise.each(tables, logAdjustment))
    // regroup indexes under their associated tables.
    .then(tables => groupTables(tables))
    // send adjusted values to DynamoDB
    .then(tables => Promise.each(tables, applyAdjustments))
    .tap(tables => log('Number of updated tables: ' + tables.length))
    .tap(tables => callback(null, 'I did it!'))
    .catch(e => {
      log('I failed: ' + JSON.stringify(e, null, '  '), true)
        .finally(_ => callback(e))
    });
}

// returns a promised array containing all table names.
function getTableNames(key, prevTableNames) {
  return dynamodb.listTables({
      ExclusiveStartTableName: key
    }).promise()
    .then(response => {
      var tableNames = (prevTableNames || []).concat(response.TableNames);
      if (response.LastEvaluatedTableName) {
        return getTableNames(response.LastEvaluatedTableName, tableNames);
      } else {
        return tableNames;
      }
    });
}

// returns a promised description of the table and its indexes.
// the return value is an array of objects, where each object
// represents the table or one of its indexes.
function table(tableName) {
  return dynamodb.describeTable({
      TableName: tableName
    }).promise()
    .then(response => {
      var tableDescription = response.Table;
      var table = mapTableDescription(tableDescription);
      var indexes = (tableDescription.GlobalSecondaryIndexes || []).map(indexDescription => mapIndexDescription(tableDescription, indexDescription));
      return Promise.all(indexes.concat(table));
    });
}

// maps an AWS table description object to a promised interface
function mapTableDescription(tableDescription) {
  return mapThroughputDescription(tableDescription.TableName, null, tableDescription.ProvisionedThroughput)
    .then(t => {
      var r = {
        TableName: tableDescription.TableName,
        IndexName: null,
        Status: tableDescription.TableStatus,
        SizeBytes: tableDescription.TableSizeBytes,
        ItemCount: tableDescription.ItemCount,
        BytesPerItem: Math.ceil(tableDescription.TableSizeBytes / tableDescription.ItemCount),
        CreatedMinutesAgo: minutesAgo(new Date(tableDescription.CreationDateTime)),
      };
      return _.extend(r, t);
    })
}

// maps an AWS index description object to a promised interface
function mapIndexDescription(tableDescription, indexDescription) {
  return mapThroughputDescription(tableDescription.TableName, indexDescription.IndexName, indexDescription.ProvisionedThroughput)
    .then(t => {
      var r = {
        TableName: tableDescription.TableName,
        IndexName: indexDescription.IndexName,
        Status: indexDescription.IndexStatus,
        SizeBytes: indexDescription.IndexSizeBytes,
        ItemCount: indexDescription.ItemCount,
        BytesPerItem: Math.ceil(indexDescription.IndexSizeBytes / indexDescription.ItemCount),
        CreatedMinutesAgo: minutesAgo(new Date(tableDescription.CreationDateTime))
      };
      return _.extend(r, t);
    });
}

// maps a table or index to a promised object containing capacity and consumption data. 
function mapThroughputDescription(tableName, indexName, throughput) {
  var r = {
    LastIncreaseDateTime: throughput.LastIncreaseDateTime ? new Date(throughput.LastIncreaseDateTime) : new Date(0),
    LastDecreaseDateTime: throughput.LastDecreaseDateTime ? new Date(throughput.LastDecreaseDateTime) : new Date(0),
    NumberOfDecreasesToday: throughput.NumberOfDecreasesToday,
    ReadCapacityUnits: throughput.ReadCapacityUnits,
    WriteCapacityUnits: throughput.WriteCapacityUnits
  };
  r.LastIncreaseMinutesAgo = minutesAgo(r.LastIncreaseDateTime);
  r.LastDecreaseMinutesAgo = minutesAgo(r.LastDecreaseDateTime);
  r.NumberDecreasesRemainingToday = 4 - r.NumberOfDecreasesToday;
  return Promise.all([
      getConsumedCapacity(tableName, indexName, 'ConsumedReadCapacityUnits', CONSUMPTION_STATISTICAL_PERIOD_MINUTES),
      getConsumedCapacity(tableName, indexName, 'ConsumedWriteCapacityUnits', CONSUMPTION_STATISTICAL_PERIOD_MINUTES)
    ])
    .then(consumed => {
      r.ConsumedReadCapacity = consumed[0];
      r.ConsumedWriteCapacity = consumed[1];
      r.RatioConsumedRead = r.ConsumedReadCapacity / r.ReadCapacityUnits;
      r.RatioConsumedWrite = r.ConsumedWriteCapacity / r.WriteCapacityUnits;
      return r;
    });
}

function minutesAgo(date) {
  return (new Date().getTime() - date.getTime()) / 60000;
}

// uses CloudWatch to provide a promised estimation of current table or index consumption.
function getConsumedCapacity(tableName, globalSecondaryIndexName, metricName, minutes) {
  var end = new Date();
  var start = new Date(end.getTime() - minutes * 60000);
  var dimensions = [{
    Name: 'TableName',
    Value: tableName
  }];
  if (globalSecondaryIndexName) dimensions.push({
    Name: 'GlobalSecondaryIndexName',
    Value: globalSecondaryIndexName
  });
  var params = {
    Namespace: 'AWS/DynamoDB',
    MetricName: metricName,
    Dimensions: dimensions,
    StartTime: start,
    EndTime: end,
    Period: 60,
    Statistics: ['Sum'],
    Unit: 'Count'
  };
  return cw.getMetricStatistics(params).promise()
    .then(statistics => (_(statistics.Datapoints).map(d => d.Sum).max() || 0) / 60);
}

// drives the getIncrease and getDecrease functions at the top of this file.
function getAdjustments(table) {
  // check for read increase
  table.ReadAdjustment = Math.max(table.ReadCapacityUnits, Math.ceil(getIncrease(
    table.CreatedMinutesAgo,
    table.LastIncreaseMinutesAgo,
    table.LastDecreaseMinutesAgo,
    table.ReadCapacityUnits,
    table.ConsumedReadCapacity,
    table.RatioConsumedRead
  )));
  // check for read decrease
  if (!table.ReadAdjustment) {
    table.ReadAdjustment = Math.min(table.ReadCapacityUnits, Math.ceil(getDecrease(
      table.CreatedMinutesAgo,
      table.LastIncreaseMinutesAgo,
      table.LastDecreaseMinutesAgo,
      table.NumberDecreasesRemainingToday,
      table.ReadCapacityUnits,
      table.ConsumedReadCapacity,
      table.RatioConsumedRead
    )));
  }
  // check for write increase
  table.WriteAdjustment = Math.max(table.WriteCapacityUnits, Math.ceil(getIncrease(
    table.CreatedMinutesAgo,
    table.LastIncreaseMinutesAgo,
    table.LastDecreaseMinutesAgo,
    table.WriteCapacityUnits,
    table.ConsumedWriteCapacity,
    table.RatioConsumedWrite
  )));
  // check for write decrease
  if (!table.WriteAdjustment) {
    table.WriteAdjustment = Math.min(table.WriteCapacityUnits, Math.ceil(getDecrease(
      table.CreatedMinutesAgo,
      table.LastIncreaseMinutesAgo,
      table.LastDecreaseMinutesAgo,
      table.NumberDecreasesRemainingToday,
      table.WriteCapacityUnits,
      table.ConsumedWriteCapacity,
      table.RatioConsumedWrite
    )));
  }
  // apply min/max value constraints
  table.ReadAdjustment = Math.min(Math.max(table.ReadAdjustment, MIN_CAPACITY), MAX_CAPACITY);
  table.WriteAdjustment = Math.min(Math.max(table.WriteAdjustment, MIN_CAPACITY), MAX_CAPACITY);
  // remove unchanged adjustments
  if (table.ReadAdjustment == table.ReadCapacityUnits) delete table.ReadAdjustment;
  if (table.WriteAdjustment == table.WriteCapacityUnits) delete table.WriteAdjustment;
  return table;
}

// groups tables and indexes back to an object
function groupTables(tables) {
  return _(tables)
    .groupBy(function(table) {
      return table.TableName;
    })
    .map(tables1 => _(tables1).reduce((a, table) => {
      if (table.IndexName) {
        a.TableName = table.TableName;
        a.indexes.push(table);
        return a;
      } else {
        return _.assign(a, table);
      }
    }, {
      indexes: []
    }))
    .value();
}

// sends a message to the log and Slack
function logAdjustment(table) {
  var message = '';
  message +=
    table.IndexName ?
    'index ' + table.IndexName :
    'table ' + table.TableName;
  message +=
    table.ReadAdjustment ?
    '; read from ' + table.ReadCapacityUnits + ' to ' + table.ReadAdjustment :
    '';
  message +=
    table.WriteAdjustment ?
    '; write from ' + table.WriteCapacityUnits + ' to ' + table.WriteAdjustment :
    '';
  message += (
      table.ReadAdjustment < table.ReadCapacityUnits ||
      table.WriteAdjustment < table.WriteCapacityUnits) ?
    '; [' + (table.NumberOfDecreasesToday + 1) + ' of 4]' :
    '';
  message +=
    '; size ' + bytesToHumanString(table.SizeBytes) +
    '; count ' + Intl.NumberFormat().format(table.ItemCount);
  message +=
    table.BytesPerItem ?
    '; avg ' + bytesToHumanString(table.BytesPerItem) :
    '';
  return log(message, true);
}

// calls DynamoDB's update table API
function applyAdjustments(table) {
  var params = {
    TableName: table.TableName
  };
  if (table.ReadAdjustment || table.WriteAdjustment) {
    params.ProvisionedThroughput = {
      ReadCapacityUnits: table.ReadAdjustment || table.ReadCapacityUnits,
      WriteCapacityUnits: table.WriteAdjustment || table.WriteCapacityUnits
    };
  }
  if (table.indexes.length > 0) {
    params.GlobalSecondaryIndexUpdates = table.indexes.map(index => {
      return {
        Update: {
          IndexName: index.IndexName,
          ProvisionedThroughput: {
            ReadCapacityUnits: index.ReadAdjustment || index.ReadCapacityUnits,
            WriteCapacityUnits: index.WriteAdjustment || index.WriteCapacityUnits
          }
        }
      };
    })
  }
  log('dynamodb updateTable params: ' + JSON.stringify(params, null, '  '));
  return dynamodb.updateTable(params).promise();
}

// sends msg to the console, and optionally to Slack
function log(msg, toSlack) {
  console.log(msg);
  if (toSlack) {
    sendToSlack(msg);
  } else {
    return Promise.resolve();
  }
}

function sendToSlack(msg) {
  if (SLACK_HOOK_URL) {
    return new Promise((resolve, reject) => {
      var slack = new Slack(SLACK_HOOK_URL);
      slack.send({ text: msg }, (a, b) => {
        if (a) reject(a);
        else resolve(b);
      });
    });
  } else {
    return Promise.resolve();
  }
}

// http://stackoverflow.com/a/14919494/1785
function bytesToHumanString(bytes, si) {
  var thresh = si ? 1000 : 1024;
  if (Math.abs(bytes) < thresh) {
    return bytes + ' B';
  }
  var units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  var u = -1;
  do {
    bytes /= thresh;
    ++u;
  } while (Math.abs(bytes) >= thresh && u < units.length - 1);
  return bytes.toFixed(1) + ' ' + units[u];
}
