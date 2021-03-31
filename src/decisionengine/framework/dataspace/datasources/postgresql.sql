DROP INDEX IF EXISTS i_channel_manager_datestamp;
DROP TABLE IF EXISTS channel_manager;
CREATE TABLE channel_manager (
   sequence_id BIGSERIAL,
   channel_manager_id character varying(36),
   name text,
   datestamp timestamp with time zone default NOW()
);

ALTER TABLE ONLY channel_manager
      ADD CONSTRAINT pk_channel_manager PRIMARY KEY (sequence_id);

CREATE INDEX i_channel_manager_datestamp ON channel_manager(datestamp);

DROP INDEX IF EXISTS i_header_channel_manager_id;
DROP TABLE IF EXISTS header;
CREATE TABLE header (
    channel_manager_id BIGINT,
    generation_id INTEGER,
    key TEXT,
    create_time BIGINT,
    expiration_time BIGINT,
    scheduled_create_time BIGINT,
    creator TEXT,
    schema_id BIGINT
    );

ALTER TABLE ONLY header
    ADD CONSTRAINT header_channel_manager_id_fkey FOREIGN KEY (channel_manager_id)
    REFERENCES channel_manager(sequence_id)
    ON UPDATE CASCADE ON DELETE CASCADE;

CREATE INDEX i_header_channel_manager_id ON header(channel_manager_id);

DROP TABLE IF EXISTS schema;
CREATE TABLE schema (
    schema_id SERIAL,
    schema BYTEA
    );

DROP INDEX IF EXISTS i_metadata_channel_manager_id;
DROP TABLE IF EXISTS metadata;
CREATE TABLE metadata (
    channel_manager_id BIGINT,
    generation_id INTEGER,
    key TEXT,
    state TEXT,
    generation_time BIGINT,
    missed_update_count INTEGER
    );

ALTER TABLE ONLY metadata
    ADD CONSTRAINT metadata_channel_manager_id_fkey FOREIGN KEY (channel_manager_id)
    REFERENCES channel_manager(sequence_id)
    ON UPDATE CASCADE ON DELETE CASCADE;

CREATE INDEX i_metadata_channel_manager_id ON metadata(channel_manager_id);

DROP INDEX IF EXISTS i_dataproduct_channel_manager_id;
DROP TABLE IF EXISTS dataproduct;
CREATE TABLE dataproduct (
    channel_manager_id BIGINT,
    generation_id INTEGER,
    key TEXT,
    value BYTEA
    );

ALTER TABLE ONLY dataproduct
    ADD CONSTRAINT dataproduct_channel_manager_id_fkey FOREIGN KEY (channel_manager_id)
    REFERENCES channel_manager(sequence_id)
    ON UPDATE CASCADE ON DELETE CASCADE;

CREATE INDEX i_dataproduct_channel_manager_id ON dataproduct(channel_manager_id);

DROP FUNCTION IF EXISTS f_id2sequence;
CREATE FUNCTION f_id2sequence(character varying) RETURNS BIGINT
    LANGUAGE sql
    AS $_$
                SELECT sequence_id FROM channel_manager WHERE channel_manager_id = $1;
            $_$;


DROP FUNCTION IF EXISTS f_get_current_channel_sequence;
CREATE FUNCTION f_get_current_channel_sequence(character varying) RETURNS BIGINT
    LANGUAGE sql
    AS $_$
                SELECT max(sequence_id) FROM channel_manager WHERE name = $1;
            $_$;

DROP FUNCTION IF EXISTS f_get_current_channel_id;
CREATE FUNCTION f_get_current_channel_id(character varying) RETURNS CHARACTER VARYING
    LANGUAGE sql
    AS $_$
                SELECT channel_manager_id FROM channel_manager
		WHERE sequence_id  = f_get_current_channel_sequence($1);
            $_$;


