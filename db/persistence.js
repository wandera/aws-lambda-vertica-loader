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
      's3prefix VARCHAR(200),' +
      'batchid VARCHAR(36),' +
      'clusterloadstatements TEXT,' +
      'clusterloadstatus TEXT,' +
      'entries VARCHAR(300)[],' +
      'lastupdate NUMERIC(13,3),' +
      'status VARCHAR(10),' +
      'errormessage TEXT,' +
      'PRIMARY KEY(s3prefix, batchid))',
    processedFiles:
      'CREATE TABLE ' + DB_STRUCTURE_NAMES.processedFilesTable + ' (' +
      'loadfile VARCHAR(300) PRIMARY KEY,' +
      'batchid VARCHAR(36))',
    config:
      'CREATE TABLE ' + DB_STRUCTURE_NAMES.configTable + ' (' +
      's3prefix VARCHAR(200) PRIMARY KEY,' +
      'batchsize INTEGER,' +
      'batchtimeoutsecs INTEGER,' +
      'copyoptions VARCHAR(200),' +
      'currentbatch VARCHAR(36),' +
      'lastupdate NUMERIC(13,3),' +
      'lastbatchrotation NUMERIC(13,3),' +
      'filenamefilterregex VARCHAR(100),' +
      'loadclusters JSON,' +
      's3mountdir VARCHAR(100),' +
      'successtopicarn VARCHAR(100),' +
      'failuretopicarn VARCHAR(100),' +
      'version VARCHAR(20))'
  },
  INDEXES: {
    // This index is needed for queryBatches.js script
    batches: 'CREATE INDEX ON ' + DB_STRUCTURE_NAMES.batchesIndex + ' (status, lastupdate)'
  }
};

const POSTGRES_ERROR = {
  DUPLICATED_KEY: '23505',
  TRANSACTION_CLASH: '40001'
};

function quote(value) {
  if (value) {
    value = value.replace(/'/g, "''");
    return "'" + value + "'";
  } else {
    return null;
  }
}

function unwrapFirstRow(data) {
  if (data && data.rowCount !== 0) {
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
  const select = "SELECT * FROM " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "WHERE " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix);

  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "entries = CASE WHEN NOT(entries::varchar[] @> ARRAY[" + quote(entity.entry) + "]::varchar[]) THEN array_append(entries, " + quote(entity.entry) + ") ELSE entries END," +
      "lastupdate = " + entity.lastupdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix);

  const insert = "INSERT INTO " + DB_STRUCTURE_NAMES.batchesTable + " (batchid, s3prefix, entries, lastupdate, status) VALUES (" +
    quote(entity.batchid) + "," +
    quote(entity.s3prefix) + "," +
    "ARRAY[" + quote(entity.entry) + "]," +
    entity.lastupdate + "," +
    quote(entity.status) + ")";

  function finishWithError(error) {
    client.query("ROLLBACK", function(err) {
      if (err) {
        console.log(err);
      }
      if (error.code === POSTGRES_ERROR.DUPLICATED_KEY || error.code === POSTGRES_ERROR.TRANSACTION_CLASH) {
        error.code = conditionCheckFailed;
      }
      callback(error);
    });
  }

  function finishWithSuccess() {
    client.query("COMMIT", function(err) {
      callback(err);
    });
  }

  function finish(err) {
    if (err) {
      finishWithError(err);
    } else {
      finishWithSuccess();
    }
  }

  client.query("BEGIN ISOLATION LEVEL SERIALIZABLE", function(err) {
    if (!err) {
      client.query(select, function(err, data) {
        if(!err) {
          if (data.rowCount === 1) {
            if (data.rows[0].status !== 'open') {
              //There is batch in the database but its not open
              finishWithError({code: conditionCheckFailed});
            } else {
              //Update existing open batch
              client.query(update, function (err) {
                finish(err);
              });
            }
          } else {
            // There is no existing batch so create new one
            client.query(insert, function (err) {
              finish(err);
            });
          }
        } else {
          finishWithError(err);
        }
      });
    } else {
      callback(err);
    }
  });
};

exports.lockBatch = function(client, entity, callback) {
  const update = "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastupdate = " + entity.lastupdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "status = 'open' AND " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix) + " " +
    "RETURNING entries";

  function transformError(err) {
    if (err && err.code === POSTGRES_ERROR.TRANSACTION_CLASH) {
      console.info("Someone else just locked the batch. No need to worry then.");
      err.code = conditionCheckFailed;
    }
    return err;
  }

  client.query("BEGIN ISOLATION LEVEL SERIALIZABLE", function (err) {
    if (!err) {
      client.query(update, function (err, data) {
        if (!err) {
          client.query("COMMIT", function (err) {
            callWithFirstRow(callback, transformError(err), data);
          });
        } else {
          client.query("ROLLBACK", function () {
            callback(transformError(err));
          })
        }
      });
    } else {
      callback(err);
    }
  });
};

exports.unlockBatch = function(client, entity, callback) {
  const update = "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastupdate = " + entity.lastupdate + "," +
      "status = " + quote(entity.status) + " " +
    "WHERE " +
      "(status = 'locked' OR status = 'error') AND " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix);

  client.query(update, function (err, data) {
    callWithFirstRow(callback, err, data);
  });
};

exports.closeBatch = function(client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastupdate = " + entity.lastupdate + "," +
      "status = " + quote(entity.status) + "," +
      "errormessage = " + (entity.errormessage ? quote(entity.errormessage) : "null") + " " +
    "WHERE " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix);

  client.query(update, callback)
};

exports.changeLoadState = function (client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "SET " +
      "lastupdate = " + entity.lastupdate + "," +
      "clusterloadstatus = " + quote(entity.clusterloadstatus) + "," +
      "clusterloadstatements = " + quote(entity.clusterloadstatements) + " " +
    "WHERE " +
      "batchid = " + quote(entity.batchid) + " AND " +
      "s3prefix = " + quote(entity.s3prefix);

  client.query(update, callback)
};

exports.getBatch = function (client, batchid, s3prefix, callback) {
  const select = "SELECT * FROM " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "WHERE " +
      "s3prefix = " + quote(s3prefix) + " AND " +
      "batchid = " + quote(batchid);

  client.query(select, function (err, data) {
    callback(err, unwrapFirstRow(data));
  });
};

exports.getBatches = function (client, status, lastupdate, callback) {
  var select = "SELECT * FROM " + DB_STRUCTURE_NAMES.batchesTable + " " +
    "WHERE status = " + quote(status);

  if (lastupdate) {
    select += " AND lastupdate >= " + lastupdate;
  }
  client.query(select, callback);
};

/*
*********
* Files *
*********
*/
exports.linkFileToBatch = function(client, file, batchid, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "SET batchid=" + quote(batchid) + " " +
    "WHERE loadfile=" + quote(file);

  client.query(update, callback);
};

exports.putFileEntry = function (client, filePath, callback) {
  const insert = "INSERT INTO " + DB_STRUCTURE_NAMES.processedFilesTable + " (loadfile) VALUES (" + quote(filePath) + ")";

  client.query(insert, callback);
};

exports.getFile = function (client, filePath, callback) {
  const select = "SELECT * FROM " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "WHERE loadfile = " + quote(filePath);

  client.query(select, function (err, data) {
    callback(err, unwrapFirstRow(data));
  });
};

exports.deleteFile = function (client, filePath, callback) {
  const del = "DELETE FROM " + DB_STRUCTURE_NAMES.processedFilesTable + " " +
    "WHERE loadfile = " + quote(filePath);

  client.query(del, callback);
};

/*
****************
* Batch config *
****************
*/
exports.putConfig = function (client, config, callback) {
  const insert =
    "INSERT INTO " + DB_STRUCTURE_NAMES.configTable + " (s3prefix, batchsize, batchtimeoutsecs, copyoptions, currentbatch, filenamefilterregex, loadclusters, s3mountdir, successtopicarn, failuretopicarn, version) " +
    "VALUES (" +
      quote(config.s3prefix) + "," +
      config.batchsize + "," +
      config.batchtimeoutsecs + "," +
      quote(config.copyoptions) + "," +
      quote(config.currentbatch) + "," +
      quote(config.filenamefilterregex) + "," +
      quote(JSON.stringify(config.loadclusters)) + "," +
      quote(config.s3mountdir) + "," +
      quote(config.successtopicarn) + "," +
      quote(config.failuretopicarn) + "," +
      quote(config.version) + ")";

  client.query(insert, callback);
};

exports.allocateBatch = function (client, entity, callback) {
  const update =
    "UPDATE " + DB_STRUCTURE_NAMES.configTable + " " +
    "SET " +
      "currentbatch=" + quote(entity.currentbatch) + ", " +
      "lastbatchrotation=" + entity.lastbatchrotation + " " +
    "WHERE s3prefix=" + quote(entity.s3prefix);

  client.query(update, callback);
};

exports.getConfig = function (client, s3prefix, callback) {
  const select =
    "SELECT * FROM " + DB_STRUCTURE_NAMES.configTable + " " +
    "WHERE s3prefix = " + quote(s3prefix);

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
      console.error(err.toString());
      client.end();
      process.exit(ERROR);
    }
  }

  console.info("Creating Tables in Postgres if Required");
  client.query(DDL.TABLES.batches, function (err) {
    handleCreateTableError(err);
    client.query(DDL.TABLES.processedFiles, function (err) {
      handleCreateTableError(err);
      client.query(DDL.TABLES.config, function (err) {
        handleCreateTableError(err);
        client.query(DDL.INDEXES.batches, function (err) {
          handleCreateTableError(err);
          if (callback) {
            callback();
          }
        });
      });
    });
  });
};

exports.dropTables = function (client, callback) {
  function handleDropTableError(err) {
    if (err && err.routine !== 'DropErrorMsgNonExistent') {
      console.error(err.toString());
      process.exit(ERROR);
    }
  }

  client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.batchesTable, function (err) {
    handleDropTableError(err);
    client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.processedFilesTable, function (err) {
      handleDropTableError(err);
      client.query('DROP TABLE ' + DB_STRUCTURE_NAMES.configTable, function (err) {
        handleDropTableError(err);
        console.info("All Configuration Tables Dropped");
        if (callback) {
          callback();
        }
      });
    });
  });
};