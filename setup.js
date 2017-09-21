/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */


/* 
 * May 2015
 *
 * Derivative created by HP, to leverage and extend the function framework to provide automatic loading from S3, via 
 * Lambda, to the HP Vertica Analytic Database platform. This derivative work remains governed by the Amazon 
 * Software License, and is subject to all terms and restrictions noted in ASL.
 *
 */



/**
 * Ask questions of the end user via STDIN and then setup the dynamo DB table
 * entry for the configuration when done
 */
var pjson = require('./package.json');
var readline = require('readline');
var aws = require('aws-sdk');
require('./constants');
require('./defaults');
require('./defaults_custom');
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var kmsCrypto = require('./kmsCrypto');
var setRegion;

configDefinition = {
	currentbatch :  uuid.v4(),
	version : pjson.version,
	loadclusters : [{}]
};

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

// connect to PostgreSQL
var postgresClient = require('./db/postgresConnector').connect();

const releaseConnection = function () {
  if (postgresClient) {
    postgresClient.end();
  }
};

process.on('SIGINT', releaseConnection);
process.on('SIGTERM', releaseConnection);

var qs = [];

q_region = function(callback) {
	rl.question('Enter the Region for the Configuration [' + dfltRegion + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltRegion;
                } 
		if (common.blank(answer) !== null) {
			common.validateArrayContains([ "ap-northeast-1", "ap-southeast-1",
					"ap-southeast-2", "eu-central-1", "eu-west-1", "sa-east-1",
					"us-east-1", "us-west-1", "us-west-2" ], answer
					.toLowerCase(), rl);

			setRegion = answer.toLowerCase();
			kmsCrypto.setRegion(setRegion);

			callback(null);
		}
	});
};

q_s3Prefix = function(callback) {
	rl.question('Enter the S3 Bucket & Prefix to watch for files [' + dfltS3Prefix + '] > ', function( answer) {
                if (common.blank(answer) === null) {
                        answer = dfltS3Prefix;
                } 
		common.validateNotNull(answer,
				'You Must Provide an S3 Bucket Name, and optionally a Prefix',
				rl);

		// setup prefix to be * if one was not provided
		var stripped = answer.replace(new RegExp('s3://', 'g'), '');
		var elements = stripped.split("/");
		var setPrefix = undefined;

		if (elements.length === 1) {
			// bucket only so use "bucket" alone
			setPrefix = elements[0];
		} else {
			// right trim "/"
			setPrefix = stripped.replace(/\/$/, '');
		}

		configDefinition.s3prefix = setPrefix;

		callback(null);
	});
};

q_s3MountDir = function(callback) {
        rl.question('Enter the s3 prefix for copy command [' + dfltS3MountDir + ']> ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltS3MountDir ;
                }
                configDefinition.s3mountdir = answer;
                callback(null);
        });
};

q_filenameFilter = function(callback) {
	rl.question('Enter a Filename Filter Regex [' + dfltFilenameFilter + ']> ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltFilenameFilter ;
                }
		// replace double \\ with single
		answer = answer.replace("\\\\","\\") ;
		if (common.blank(answer) !== null) {
			configDefinition.filenamefilterregex = answer;
		}
		callback(null);
	});
};

q_clusterEndpoint = function(callback) {
	rl.question('Enter the Vertica Cluster Endpoint (Public IP or DNS name) [' + dfltClusterEndpoint + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltClusterEndpoint ;
                }
		common.validateNotNull(answer, 'You Must Provide a Vertica Cluster Endpoint',
				rl);
		configDefinition.loadclusters[0].clusterEndpoint = answer;
		callback(null);
	});
};

q_clusterPort = function(callback) {
	rl.question('Enter the Vertica Cluster Port [' + dfltClusterPort + ']> ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltClusterPort ;
                }
		configDefinition.loadclusters[0].clusterPort = common.getIntValue(answer, rl);
		callback(null);
	});
};

q_userName = function(callback) {
	rl.question('Enter the Vertica Database Username [' + dfltUserName + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltUserName ;
                }
		common.validateNotNull(answer, 'You Must Provide a Username', rl);
		configDefinition.loadclusters[0].connectUser = answer;
		callback(null);
	});
};

q_userPwd = function(callback) {
	rl.question('Enter the Vertica Database Password [' + dfltUserPwd + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltUserPwd ;
                }
		common.validateNotNull(answer, 'You Must Provide a Password', rl);
		kmsCrypto.encrypt(answer, function(err, ciphertext) {
			configDefinition.loadclusters[0].connectPassword = kmsCrypto.toLambdaStringFormat(ciphertext);
			callback(null);
		});
	});
};



q_table = function(callback) {
	rl.question('Enter the Table to be Loaded [' + dfltTable + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltTable ;
                }
		common.validateNotNull(answer, 'You Must Provide a Table Name', rl);
		configDefinition.loadclusters[0].targetTable = answer;
		callback(null);
	});
};

q_copyOptions = function(callback) {
	rl.question('Load Options - COPY table FROM files [*options*] [' + dfltCopyOptions + ']> ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltCopyOptions ;
                }
		if (common.blank(answer) !== null) {
			configDefinition.copyoptions = answer;
		}
		callback(null);
	});
};


q_columns = function(callback) {
  rl.question('Copy Columns - COPY table ([*columns*]) FROM files ... [' + dfltColumns + ']> ', function(answer) {
    if (common.blank(answer) === null) {
      answer = dfltColumns;
    }
    if (common.blank(answer) !== null) {
      configDefinition.loadclusters[0].copyColumns = answer;
    }
    callback(null);
  });
};

q_preLoadStatement = function(callback) {
  rl.question('Enter SQL statement to run before the load [' + dfltPreLoadStatement + ']> ', function (answer) {
    if (common.blank(answer) === null) {
      answer = dfltPreLoadStatement;
    }
    if (common.blank(answer) !== null) {
      configDefinition.loadclusters[0].preLoadStatement = answer;
    }
		callback(null);
	});
};

q_postLoadStatement = function (callback) {
  rl.question('Enter SQL statement to run after the load [' + dfltPostLoadStatement + ']> ', function (answer) {
    if (common.blank(answer) === null) {
      answer = dfltPostLoadStatement;
    }
    if (common.blank(answer) !== null) {
      configDefinition.loadclusters[0].postLoadStatement = answer;
    }
    callback(null);
  });
};


q_batchSize = function(callback) {
	rl.question('How many files should be buffered before loading? [' + dfltBatchSize + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltBatchSize ;
                }
		if (common.blank(answer) !== null) {
			configDefinition.batchsize =  common.getIntValue(answer, rl);
		}
		callback(null);
	});
};

q_batchTimeoutSecs = function(callback) {
	rl.question('How old should we allow a Batch to be before loading (seconds)? [' + dfltBatchTimeoutSecs + ']> ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltBatchTimeoutSecs ;
                }
		if (common.blank(answer) !== null) {
			configDefinition.batchtimeoutsecs = common.getIntValue(answer, rl);
		}
		callback(null);
	});
};

q_successTopic = function(callback) {
	rl.question('Enter the SNS Topic ARN for Successful Loads [' + dfltSuccessTopic + '] > ', function( answer) {
                if (common.blank(answer) === null) {
                        answer = dfltSuccessTopic ;
                }
		if (common.blank(answer) !== null) {
			configDefinition.successtopicarn = answer;
		}
		callback(null);
	});
};

q_failureTopic = function(callback) {
	rl.question('Enter the SNS Topic ARN for Failed Loads [' + dfltFailureTopic + '] > ', function(answer) {
                if (common.blank(answer) === null) {
                        answer = dfltFailureTopic ;
                }
		if (common.blank(answer) !== null) {
			configDefinition.failuretopicarn = answer;
		}
		callback(null);
	});
};


last = function(callback) {
	rl.close();

	setup(null, callback);
};

setup = function(overrideConfig, callback) {
	// set which configuration to use
	var useConfig = undefined;
	if (overrideConfig) {
		useConfig = overrideConfig;
	} else {
		useConfig = configDefinition;
	}

	var innerCallback = function(err) {
		postgresClient.end();
		callback(err);
	};

	var configWriter = common.writeConfig(setRegion, postgresClient, useConfig, innerCallback);
	common.createTables(postgresClient, configWriter);
};
// export the setup module so that customers can programmatically add new
// configurations
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_s3MountDir);
qs.push(q_filenameFilter);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_table);
qs.push(q_copyOptions);
qs.push(q_columns);
qs.push(q_preLoadStatement);
qs.push(q_postLoadStatement);
qs.push(q_batchSize);
qs.push(q_batchTimeoutSecs);
qs.push(q_userName);
qs.push(q_userPwd);
qs.push(q_successTopic);
qs.push(q_failureTopic);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);
