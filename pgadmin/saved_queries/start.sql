CREATE DATABASE "ndb";

\c ndb;

SELECT create_hypertable('ndb', 'column', chunk_time_interval => interval '1 day');
