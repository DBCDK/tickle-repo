/*
Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
See license text in LICENSE.txt
*/

CREATE TABLE dataset (
  id            SERIAL PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL,
  displayName   TEXT UNIQUE
);
CREATE INDEX dataset_name_index ON dataset(name);

CREATE TYPE batch_type AS ENUM ('INCREMENTAL', 'TOTAL');

CREATE TABLE batch (
  id                  SERIAL PRIMARY KEY,
  dataset             INTEGER REFERENCES dataset(id) ON DELETE CASCADE,
  batchKey            INTEGER UNIQUE NOT NULL,
  type                batch_type NOT NULL,
  timeOfCreation      TIMESTAMP DEFAULT now(),
  timeOfCompletion    TIMESTAMP
);
CREATE INDEX batch_dataset_index ON batch(dataset);
CREATE INDEX batch_batchKey_index ON batch(batchKey);

CREATE TYPE record_status AS ENUM ('ACTIVE', 'DELETED', 'RESET');

CREATE TABLE record (
  id                      SERIAL PRIMARY KEY,
  batch                   INTEGER REFERENCES batch(id) ON DELETE SET NULL,
  dataset                 INTEGER REFERENCES dataset(id) ON DELETE SET NULL,
  agencyId                INTEGER NOT NULL,
  localId                 TEXT NOT NULL,
  trackingId              TEXT NOT NULL,
  status                  record_status NOT NULL,
  timeOfCreation          TIMESTAMP DEFAULT clock_timestamp(),
  timeOfLastModification  TIMESTAMP,
  content                 BYTEA NOT NULL,
  chksum                  TEXT NOT NULL
);
CREATE INDEX record_batch_index ON record(batch);
CREATE INDEX record_dataset_index ON record(dataset);
CREATE INDEX record_status_index ON record(status);
