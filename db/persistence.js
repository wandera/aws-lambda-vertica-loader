const DB_STRUCTURE_NAMES = {
  batchesTable: 'LambdaVerticaBatches',
  processedFilesTable: 'LambdaVerticaProcessedFiles',
  configTable: 'LambdaVerticaBatchLoadConfig',
  batchesIndex: 'LambdaVerticaBatches'
};
const DDL = {
  TABLES: {
    batches:
      'CREATE TABLE ' + DB_STRUCTURE_NAMES.batchesTable + ' (' +
      's3Prefix VARCHAR(200) PRIMARY KEY,' +
      'batchId VARCHAR(36),' +
      'clusterLoadStatements TEXT,' +
      'clusterLoadStatus TEXT,' +
      'entries VARCHAR(300)[],' +
      'lastUpdate NUMERIC(13,3),' +
      'status VARCHAR(10),' +
      'errorMessage TEXT)',
    processedFiles:
      'CREATE TABLE ' + DB_STRUCTURE_NAMES.processedFilesTable + ' (' +
      'loadFile VARCHAR(300) PRIMARY KEY,' +
      'batchId VARCHAR(36))',
    config:
      'CREATE TABLE ' + DB_STRUCTURE_NAMES.configTable + ' (' +
      's3Prefix VARCHAR(200) PRIMARY KEY,' +
      'batchSize INTEGER,' +
      'batchTimeoutSecs INTEGER,' +
      'copyOptions VARCHAR(200),' +
      'currentBatch VARCHAR(36),' +
      'lastUpdate NUMERIC(13,3),' +
      'filenameFilterRegex VARCHAR(100),' +
      'loadClusters JSON,' +
      's3MountDir VARCHAR(100),' +
      'successTopicARN VARCHAR(100),' +
      'failureTopicARN VARCHAR(100),' +
      'version VARCHAR(20))'
  },
  INDEXES: {
    batches: 'CREATE INDEX ON ' + DB_STRUCTURE_NAMES.batchesIndex + ' (batchId)'
  }
};

function quote(value) {
  return "'" + value + "'";
}

function unwrapFirstRow(data) {
  if (data.rowCount !== 0) {
    return data.rows[0];
  }
}

function callWithFirstRow(callback, err, data) {
  if (!err && data.rowCount === 0) {
    err = {
      code: conditionCheckFailed
    };
  }
  callback(err, unwrapFirstRow(data));
}

/*
***********
* Batches *
***********
*/
exports.updateBatch = function(client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "entries = entries || " + quote(entity.entry) + "," +
      "lastUpdate = " + entity.lastUpdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "(status = 'open' OR status is null) AND " +
      "batchId = " + quote(entity.batchId) + " AND " +
      "s3Prefix = " + quote(entity.s3Prefix) + " " +
    "RETURNING entries";

  client.query(update, function (err, data) {
    callWithFirstRow(callback, err, data);
  });
};

exports.lockBatch = function(client, entity, callback) {
  const update = "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastUpdate = " + entity.lastUpdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "status = 'open' AND " +
      "batchId = " + quote(entity.batchId) + " AND " +
      "s3Prefix = " + quote(entity.s3Prefix) +
    "RETURNING entries";

  client.query(update, function (err, data) {
    callWithFirstRow(callback, err, data);
  });
};

exports.unlockBatch = function(client, entity, callback) {
  const update = "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastUpdate = " + entity.lastUpdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "(status = 'locked' OR status is 'error') AND " +
      "batchId = " + quote(entity.batchId) + " AND " +
      "s3Prefix = " + quote(entity.s3Prefix);

  client.query(update, function (err, data) {
    callWithFirstRow(callback, err, data);
  });
};

exports.closeBatch = function(client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastUpdate = " + entity.lastUpdate + "," +
      "status = " + quote(entity.status) + "," +
      "errorMessage = " + entity.errorMessage ? quote(entity.errorMessage) : "null" +
    "WHERE " +
      "batchId = " + quote(entity.batchId) + " AND " +
      "s3Prefix = " + quote(entity.s3Prefix);

  client.query(update, callback)
};

exports.changeLoadState = function (client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastUpdate = " + entity.lastUpdate + "," +
      "clusterLoadStatus = " + quote(entity.clusterLoadStatus) + "," +
      "clusterLoadStatements = " + quote(entity.clusterLoadStatements) + " " +
    "WHERE " +
      "batchId = " + quote(entity.batchId) + " AND " +
      "s3Prefix = " + quote(entity.s3Prefix);

  client.query(update, callback)
};

exports.getBatch = function (client, batchId, s3Prefix, callback) {
  const select = "SELECT * FROM " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "WHERE " +
      "s3Prefix = " + quote(s3Prefix) + ", " +
      "batchId = " + quote(batchId);

  client.query(select, function (err, data) {
    callback(err, unwrapFirstRow(data));
  });
};

/*
*********
* Files *
*********
*/
exports.linkFileToBatch = function(client, file, batchId, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "SET batchId=" + quote(batchId) + " " +
    "WHERE loadFile=" + quote(file);

  client.query(update, callback);
};

exports.putFileEntry = function (client, filePath, callback) {
  const insert = "INSERT INTO " + DB_STRUCTURE_NAMES.processedFilesTable + " (loadFile) VALUES (" + quote(filePath) + ")";

  client.query(insert, callback);
};

exports.getFile = function (client, filePath, callback) {
  const select = "SELECT * FROM " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "WHERE loadFile = " + quote(filePath);

  client.query(select, function (err, data) {
    callback(err, unwrapFirstRow(data));
  });
};

exports.deleteFile = function (client, filePath, callback) {
  const del = "DELETE FROM " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "WHERE loadFile = " + quote(filePath);

  client.query(del, callback);
};

/*
****************
* Batch config *
****************
*/
exports.putConfig = function (client, config, callback) {
  const insert =
    "INSERT INTO " + DB_STRUCTURE_NAMES.configTable + " (s3Prefix, batchSize, batchTimeoutSecs, copyOptions, currentBatch, filenameFilterRegex, loadClusters, s3MountDir, version) " +
    "VALUES (" +
      quote(config.s3Prefix) + "," +
      config.batchSize + "," +
      config.batchTimeoutSecs + "," +
      quote(config.copyOptions) + "," +
      quote(config.currentBatch) + "," +
      quote(config.filenameFilterRegex) + "," +
      quote(JSON.stringify(config.loadClusters)) + "," +
      quote(config.s3MountDir) + "," +
      quote(config.version) + ")";

  client.query(insert, callback);
};

exports.allocateBatch = function (client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.configTable + " " +
    "SET " +
      "currentBatch=" + quote(entity.currentBatch) + ", " +
      "lastBatchRotation=" + quote(entity.lastBatchRotation) + " " +
    "WHERE s3Prefix=" + quote(entity.s3Prefix);

  client.query(update, callback);
};

exports.getConfig = function (client, s3Prefix, callback) {
  const select =
    "SELECT * FROM " + DB_STRUCTURE_NAMES.configTable + " " +
    "WHERE s3Prefix = " + quote(s3Prefix);

  client.query(select, function (err, data) {
    callback(err, unwrapFirstRow(data));
  });
};

/*
******************
* DDL Operations *
******************
*/
exports.createTables = function (client, callback) {
  function handleCreateTableError(err) {
    if (err) {
      console.log(err.toString());
      process.exit(ERROR);
    }
  }

  console.log("Creating Tables in Dynamo DB if Required");
  client.query(DDL.TABLES.batches, function (err) {
    handleCreateTableError(err);
    client.query(DDL.TABLES.processedFiles, function (err) {
      handleCreateTableError(err);
      client.query(DDL.TABLES.config, function (err) {
        handleCreateTableError(err);
        client.query(DDL.INDEXES.batches, function (err) {
          handleCreateTableError(err);
          if (callback) {
            setTimeout(callback(), 5000);
          }
        });
      });
    });
  });
};

exports.deleteTables = function (client, callback) {
  function handleDropTableError(err) {
    if (err && err.routine !== 'DropErrorMsgNonExistent') {
      console.log(err.toString());
      process.exit(ERROR);
    }
  }

  client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.batchesTable, function (err) {
    handleDropTableError(err);
    client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.processedFilesTable, function (err) {
      handleDropTableError(err);
      client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.configTable, function (err) {
        handleDropTableError(err);
        console.log("All Configuration Tables Dropped");
        if (callback) {
          callback();
        }
      });
    });
  });
};