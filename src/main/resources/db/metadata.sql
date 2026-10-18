CREATE TABLE mf_run (
 run_id VARCHAR(128) PRIMARY KEY,
 registration_digest VARCHAR(64) NOT NULL,
 resolved_digest VARCHAR(64) NOT NULL,
 phase VARCHAR(32) NOT NULL,
 revision BIGINT NOT NULL CHECK (revision >= 0),
 cancel_requested BOOLEAN NOT NULL,
 deadline VARCHAR(40) NOT NULL,
 key_payload VARCHAR(4096),
 load_id VARCHAR(128), candidate_digest VARCHAR(64), expected_revision BIGINT,
 CHECK ((key_payload IS NULL AND load_id IS NULL AND candidate_digest IS NULL AND expected_revision IS NULL)
 OR (key_payload IS NOT NULL AND load_id IS NOT NULL AND candidate_digest IS NOT NULL AND expected_revision IS NOT NULL AND expected_revision >= 0))
);
CREATE TABLE mf_stage (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id), stage_type VARCHAR(32) NOT NULL, input_alias VARCHAR(128) NOT NULL,
 identity_digest VARCHAR(64) NOT NULL, identity_body VARCHAR(4096) NOT NULL, state VARCHAR(16) NOT NULL CHECK (state IN ('ACTIVE','COMPLETED')),
 PRIMARY KEY (run_id, stage_type, input_alias)
);
CREATE TABLE mf_manifest (
 manifest_id VARCHAR(128) PRIMARY KEY, run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id),
 stage_type VARCHAR(32) NOT NULL, input_alias VARCHAR(128) NOT NULL, identity_digest VARCHAR(64) NOT NULL,
 object_key VARCHAR(2048) NOT NULL, content_digest VARCHAR(64) NOT NULL,
 state VARCHAR(16) NOT NULL CHECK (state = 'SEALED'), sealed_at VARCHAR(40) NOT NULL,
 UNIQUE (run_id, stage_type, input_alias)
);
CREATE TABLE mf_next_intent (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id), stage_type VARCHAR(32) NOT NULL, input_alias VARCHAR(128) NOT NULL,
 consumed_digest VARCHAR(64) NOT NULL, PRIMARY KEY (run_id,stage_type,input_alias)
);
CREATE TABLE mf_required_check (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id), check_id VARCHAR(128) NOT NULL, rule_digest VARCHAR(64) NOT NULL,
 PRIMARY KEY (run_id,check_id)
);
CREATE TABLE mf_check_result (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id), identity_digest VARCHAR(64) NOT NULL, check_id VARCHAR(128) NOT NULL,
 rule_digest VARCHAR(64) NOT NULL, candidate_digest VARCHAR(64) NOT NULL, load_id VARCHAR(128) NOT NULL,
 status VARCHAR(16) NOT NULL CHECK (status IN ('PASS','FAIL','ERROR','SKIPPED')),
 content_digest VARCHAR(64) NOT NULL, object_key VARCHAR(2048) NOT NULL, object_digest VARCHAR(64) NOT NULL,
 PRIMARY KEY (run_id,identity_digest,check_id)
);
CREATE TABLE mf_publication_intent (
 publication_id VARCHAR(128) PRIMARY KEY, run_id VARCHAR(128) NOT NULL UNIQUE REFERENCES mf_run(run_id),
 key_payload VARCHAR(4096) NOT NULL, load_id VARCHAR(128) NOT NULL, candidate_digest VARCHAR(64) NOT NULL,
 expected_revision BIGINT NOT NULL CHECK (expected_revision >= 0), request_digest VARCHAR(64) NOT NULL, authorized_at VARCHAR(40) NOT NULL
);
CREATE TABLE mf_event (
 event_id VARCHAR(36) PRIMARY KEY, run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id),
 event_kind VARCHAR(40) NOT NULL, content_digest VARCHAR(64) NOT NULL, recorded_at VARCHAR(40) NOT NULL
);
CREATE TABLE mf_publication_result (
 run_id VARCHAR(128) PRIMARY KEY REFERENCES mf_run(run_id), publication_id VARCHAR(128) NOT NULL UNIQUE REFERENCES mf_publication_intent(publication_id),
 request_digest VARCHAR(64) NOT NULL, receipt_digest VARCHAR(64) NOT NULL, previous_load_id VARCHAR(128),
 revision BIGINT NOT NULL CHECK (revision>0), recorded_at VARCHAR(40) NOT NULL
);
