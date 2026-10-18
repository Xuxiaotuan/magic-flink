CREATE TABLE mf_run (
 run_id VARCHAR(128) PRIMARY KEY,
 registration_digest VARCHAR(64) NOT NULL,
 resolved_digest VARCHAR(64) NOT NULL,
 phase VARCHAR(32) NOT NULL,
 revision BIGINT NOT NULL CHECK (revision >= 0),
 cancel_requested BOOLEAN NOT NULL,
 deadline VARCHAR(40) NOT NULL, max_stage_retries INTEGER NOT NULL CHECK(max_stage_retries>=0), retry_backoff VARCHAR(40) NOT NULL,stage_timeout VARCHAR(40) NOT NULL,evidence_wait VARCHAR(40) NOT NULL,
 key_payload VARCHAR(4096),
 load_id VARCHAR(128), candidate_digest VARCHAR(64), expected_revision BIGINT,
 CHECK ((key_payload IS NULL AND load_id IS NULL AND candidate_digest IS NULL AND expected_revision IS NULL)
 OR (key_payload IS NOT NULL AND load_id IS NOT NULL AND candidate_digest IS NOT NULL AND expected_revision IS NOT NULL AND expected_revision >= 0))
);
CREATE TABLE mf_attempt (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id),stage_type VARCHAR(32) NOT NULL,input_alias VARCHAR(128) NOT NULL,attempt_id VARCHAR(128) NOT NULL,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0),executor VARCHAR(16) NOT NULL,plan_digest VARCHAR(64) NOT NULL,resolved_digest VARCHAR(64) NOT NULL,consumed_digest VARCHAR(64) NOT NULL,
 deadline VARCHAR(40) NOT NULL,created_at VARCHAR(40) NOT NULL,plan_body VARCHAR(8192),definition_digest VARCHAR(64) NOT NULL,
 identity_digest VARCHAR(64),identity_body VARCHAR(4096),
 execution_state VARCHAR(16) NOT NULL CHECK(execution_state IN ('PLANNED','SUBMITTING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
 completion_state VARCHAR(16) NOT NULL CHECK(completion_state IN ('ACTIVE','COMPLETED')),
 dispatch_id VARCHAR(128),dispatch_started_at VARCHAR(40),stop_digest VARCHAR(64),stop_reason VARCHAR(32),stopped_at VARCHAR(40),replaced_by VARCHAR(128),parent_attempt_id VARCHAR(128),
 PRIMARY KEY(run_id,stage_type,input_alias,attempt_id),UNIQUE(run_id,stage_type,input_alias,ordinal),
 FOREIGN KEY(run_id,stage_type,input_alias,replaced_by) REFERENCES mf_attempt(run_id,stage_type,input_alias,attempt_id),
 FOREIGN KEY(run_id,stage_type,input_alias,parent_attempt_id) REFERENCES mf_attempt(run_id,stage_type,input_alias,attempt_id),
 CHECK((identity_digest IS NULL AND identity_body IS NULL) OR (identity_digest IS NOT NULL AND identity_body IS NOT NULL))
);
CREATE TABLE mf_stage (
 run_id VARCHAR(128) NOT NULL REFERENCES mf_run(run_id), stage_type VARCHAR(32) NOT NULL, input_alias VARCHAR(128) NOT NULL,current_attempt_id VARCHAR(128) NOT NULL,
 identity_digest VARCHAR(64), identity_body VARCHAR(4096), state VARCHAR(16) NOT NULL CHECK (state IN ('ACTIVE','COMPLETED')),
 PRIMARY KEY (run_id, stage_type, input_alias),
 FOREIGN KEY(run_id,stage_type,input_alias,current_attempt_id) REFERENCES mf_attempt(run_id,stage_type,input_alias,attempt_id)
);
CREATE TABLE mf_stop_evidence (
 content_digest VARCHAR(64) PRIMARY KEY,run_id VARCHAR(128) NOT NULL,stage_type VARCHAR(32) NOT NULL,input_alias VARCHAR(128) NOT NULL,attempt_id VARCHAR(128) NOT NULL,
 content_body VARCHAR(16384) NOT NULL,
 FOREIGN KEY(run_id,stage_type,input_alias,attempt_id) REFERENCES mf_attempt(run_id,stage_type,input_alias,attempt_id)
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
 consumed_digest VARCHAR(64) NOT NULL,consumed_body VARCHAR(16384) NOT NULL,first_attempt_id VARCHAR(128),cancelled BOOLEAN NOT NULL,
 FOREIGN KEY(run_id,stage_type,input_alias,first_attempt_id) REFERENCES mf_attempt(run_id,stage_type,input_alias,attempt_id),
 PRIMARY KEY (run_id,stage_type,input_alias)
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
