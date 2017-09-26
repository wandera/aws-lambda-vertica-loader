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


var aws = require('aws-sdk');
require('./constants');
var common = require('./common');

if (process.argv.length < 4) {
	console.log("You must provide an AWS Region Code, Batch Status, and optionally a start time to query from");
	process.exit(ERROR);
}
var batchStatus = process.argv[3];
var startDate;

// use date parse to get a start time, using supported date format from
// javascript - us format only :(
if (process.argv.length > 4) {
	var ms = Date.parse(process.argv[4]);
	if (!isNaN(ms)) {
		startDate = ms / 1000;
	}
}
// connect to PostgreSQL
var Persistence = require('./db/persistence');
var postgresClient = require('./db/postgresConnector').connect();

function exit(code) {
  postgresClient.end();
  process.exit(code);
}

Persistence.getBatches(postgresClient, batchStatus, startDate, function(err, data) {
	if (err) {
		console.log(err);
		exit(ERROR);
	} else {
		if (data && data.rows) {
			var itemsToShow = [];

			for (var i = 0; i < data.rows.length; i++) {
				toShow = {
					s3prefix : data.rows[i].s3prefix,
					batchid : data.rows[i].batchid,
					lastupdateDate : common.readableTime(data.rows[i].lastupdate)
				};
				itemsToShow.push(toShow);
			}

			console.log(JSON.stringify(itemsToShow));
			exit(OK);
		} else {
			console.log("Unable to query Batch Status");
			exit(ERROR);
		}
	}
});
