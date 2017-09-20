const pg = require('pg');
const Client = pg.Client;

exports.connect = function() {
  const client = new Client({
    user: 'pass',
    host: 'localhost',
    database: 'pass',
    password: 'pass',
    port: 5432
  });

  client.connect();

  return client;
};