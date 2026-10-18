package io.github.xuxiaotuan.magicflink.control;

import java.util.Objects;
import java.util.Optional;

/** Trusted, frozen plan identity. Task attempt identity belongs to the future Flink file adapter. */
public record StageIdentity(StageKey stage, String attemptId, Executor executor,
                            Optional<FlinkResource> resource, String planDigest,
                            String resolvedSpecDigest, String consumedDataDigest) {
    public StageIdentity {
        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(resource, "resource");
        requireText(attemptId); requireText(planDigest); requireText(resolvedSpecDigest); requireText(consumedDataDigest);
        if ((executor == Executor.FLINK) != resource.isPresent())
            throw new IllegalArgumentException("Flink requires CR UID and Job ID; INLINE forbids them");
    }

    public enum Executor { FLINK, INLINE }
    public enum StageType { COLLECT, PRECHECK, COMPUTE, RESULT_CHECK, WRITE, TARGET_CHECK }
    public record StageKey(String runId, StageType type, Optional<String> inputAlias) {
        public StageKey {
            requireText(runId);
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(inputAlias, "inputAlias");
            inputAlias.ifPresent(StageIdentity::requireText);
            if ((type == StageType.COLLECT) != inputAlias.isPresent())
                throw new IllegalArgumentException("only COLLECT requires inputAlias");
        }
    }
    public record FlinkResource(String crUid, String jobId) {
        public FlinkResource { requireText(crUid); requireText(jobId); }
    }
    static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("identity/digest must be nonblank");
    }
}
