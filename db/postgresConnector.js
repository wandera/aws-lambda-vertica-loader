const pg = require('pg');
const Client = pg.Client;

exports.connect = function() {
  const client = new Client({
    user: process.env.postgres_user,
    host: process.env.postgres_host,
    database: process.env.postgres_database,
    password: process.env.postgres_password,
    port: process.env.postgres_port
  });

  client.connect();

  return client;
};