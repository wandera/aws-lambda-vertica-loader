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


var async = require('async');
var Persistence = require('./db/persistence');
require('./constants');
require('./defaults');


// function which creates a string representation of now suitable for use in S3
// paths
exports.getFormattedDate = function(date) {
	if (!date) {
		date = new Date();
	}

	var hour = date.getHours();
	hour = (hour < 10 ? "0" : "") + hour;

	var min = date.getMinutes();
	min = (min < 10 ? "0" : "") + min;

	var sec = date.getSeconds();
	sec = (sec < 10 ? "0" : "") + sec;

	var year = date.getFullYear();

	var month = date.getMonth() + 1;
	month = (month < 10 ? "0" : "") + month;

	var day = date.getDate();
	day = (day < 10 ? "0" : "") + day;

	return year + "-" + month + "-" + day + "-" + hour + ":" + min + ":" + sec;
};

/* current time as seconds */
exports.now = function() {
	return new Date().getTime() / 1000;
};

exports.readableTime = function(epochSeconds) {
	var d = new Date(0);
	d.setUTCSeconds(epochSeconds);
	return exports.getFormattedDate(d);
};

exports.createTables = function(postgresClient, callback) {
	Persistence.createTables(postgresClient, callback);
};

exports.writeConfig = function(setRegion, postgresClient, config, outerCallback) {
  return function () {
    Persistence.putConfig(postgresClient, config, function(err, data) {
      if (err) {
        console.log(JSON.stringify(config));
        console.log(JSON.stringify(err));
        outerCallback(err);
      } else {
        if (data) {
          console.log("Configuration for " + config.s3prefix + " successfully written in " + setRegion);
          outerCallback(null);
        }
      }
    });
	}
};

exports.dropTables = function(postgresClient, callback) {
	// drop the config table
	console.log("Dropping database tables...");
	Persistence.dropTables(postgresClient, callback);
};

/* validate that the given value is a number, and if so return it */
exports.getIntValue = function(value, rl) {
	if (!value || value === null) {
		rl.close();
		console.log('Null Value');
		process.exit(INVALID_ARG);
	} else {
		var num = parseInt(value);

		if (isNaN(num)) {
			rl.close();
			console.log('Value \'' + value + '\' is not a Number');
			process.exit(INVALID_ARG);
		} else {
			return num;
		}
	}
};

exports.getBooleanValue = function(value) {
	if (value) {
		if ([ 'TRUE', '1', 'YES', 'Y' ].indexOf(value.toUpperCase()) > -1) {
			return true;
		} else {
			return false;
		}
	} else {
		return false;
	}
};

/* validate that the provided value is not null/undefined */
exports.validateNotNull = function(value, message, rl) {
	if (!value || value === null || value === '' || value === REQD_BLANK || value === OPTIONAL_BLANK ) {
		rl.close();
		console.log(message);
		process.exit(INVALID_ARG);
	}
};

/* turn blank lines read from STDIN to Null */
exports.blank = function(value) {
	if (value === '' || value === REQD_BLANK || value === OPTIONAL_BLANK ) {
		return null;
	} else {
		return value;
	}
};

exports.validateArrayContains = function(array, value, rl) {
	if (!(array.indexOf(value) > -1)) {
		rl.close();
		console.log('Value must be one of ' + array.toString());
		process.exit(INVALID_ARG);
	}
};

exports.randomInt = function(low, high) {
	return Math.floor(Math.random() * (high - low) + low);
};
