CREATE TABLE mf_target_version (
 load_id VARCHAR(128) PRIMARY KEY, business_key VARCHAR(64) NOT NULL, candidate_digest VARCHAR(64) NOT NULL,
 state VARCHAR(16) NOT NULL CHECK (state IN ('WRITING','SEALED','ACCEPTED','REJECTED'))
);
CREATE TABLE mf_publish_pointer (
 business_key VARCHAR(64) PRIMARY KEY, revision BIGINT NOT NULL CHECK (revision >= 0),
 active_load_id VARCHAR(128) REFERENCES mf_target_version(load_id), publication_id VARCHAR(128),
 CHECK ((revision=0 AND active_load_id IS NULL AND publication_id IS NULL)
 OR (revision>0 AND active_load_id IS NOT NULL AND publication_id IS NOT NULL))
);
CREATE TABLE mf_publish_history (
 publication_id VARCHAR(128) PRIMARY KEY, run_id VARCHAR(128) NOT NULL,
 business_key VARCHAR(64) NOT NULL REFERENCES mf_publish_pointer(business_key), key_payload VARCHAR(4096) NOT NULL,
 load_id VARCHAR(128) NOT NULL REFERENCES mf_target_version(load_id), candidate_digest VARCHAR(64) NOT NULL,
 expected_revision BIGINT NOT NULL CHECK (expected_revision >= 0), request_digest VARCHAR(64) NOT NULL,
 previous_load_id VARCHAR(128), revision BIGINT NOT NULL CHECK (revision = expected_revision+1), recorded_at VARCHAR(40) NOT NULL,
 UNIQUE (business_key,revision)
);
