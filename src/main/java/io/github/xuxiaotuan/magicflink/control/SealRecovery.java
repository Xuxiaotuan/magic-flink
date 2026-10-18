package io.github.xuxiaotuan.magicflink.control;

import java.util.Objects;

import static io.github.xuxiaotuan.magicflink.control.ReceiptGate.*;

/** Chooses the next action from trusted observations. Performs no I/O and grants no durable write authorization. */
public final class SealRecovery {
    private SealRecovery() {}
    public enum Terminal { UNKNOWN, RUNNING, SUCCEEDED, FAILED, CANCELLED }
    public enum Evidence { UNKNOWN, ABSENT, INCOMPLETE, COMPLETE, CONFLICT }
    public enum FileCheck { NOT_CHECKED, UNKNOWN, VALID, CORRUPT }
    public enum Manifest { UNKNOWN, ABSENT, MATCHING, CONFLICT }
    public enum Receipt { UNKNOWN, ABSENT, MATCHING, CONFLICT }
    public enum SealDecision {
        ARCHIVE_ONLY, REREAD, WAIT_UNKNOWN, WAIT_EVIDENCE, WAIT_STOP,
        PERSIST_SUCCESS, VERIFY_FILES, BUILD_MANIFEST, SEAL_METADATA, REBUILD_RECEIPT,
        DONE, ABANDON_AND_RETRY, REJECT
    }
    /**
     * All facts must refer to identity and the same immutable evidence/manifest content.
     * MATCHING and VALID mean verified, not merely present. successDurable is a persisted success observation.
     * Deadline is derived from the fixed plan; executionStopped needs positive external confirmation.
     */
    public record Observation(StageIdentity identity, Terminal terminal, boolean successDurable,
                              Evidence evidence, FileCheck files, Manifest manifest, boolean metadataSealed,
                              Receipt receipt, boolean executionStopped, boolean evidenceDeadlineExpired) {
        public Observation {
            Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(terminal, "terminal");
            Objects.requireNonNull(evidence, "evidence"); Objects.requireNonNull(files, "files");
            Objects.requireNonNull(manifest, "manifest"); Objects.requireNonNull(receipt, "receipt");
        }
    }

    public static SealDecision decide(Current current, long observedRevision, Observation facts) {
        Objects.requireNonNull(facts, "facts");
        ReceiptDecision eligibility = ReceiptGate.qualify(current, observedRevision, facts.identity());
        if (eligibility == ReceiptDecision.REREAD) return SealDecision.REREAD;
        if (eligibility == ReceiptDecision.ARCHIVE_ONLY) return SealDecision.ARCHIVE_ONLY;
        var stage = facts.identity().stage().type();
        if (facts.identity().executor() != StageIdentity.Executor.FLINK
                || (stage != StageIdentity.StageType.COLLECT && stage != StageIdentity.StageType.COMPUTE))
            return SealDecision.REJECT;
        if (facts.evidence() == Evidence.CONFLICT || facts.manifest() == Manifest.CONFLICT
                || facts.receipt() == Receipt.CONFLICT || facts.files() == FileCheck.CORRUPT) return SealDecision.REJECT;
        boolean failed = facts.terminal() == Terminal.FAILED || facts.terminal() == Terminal.CANCELLED;
        if (facts.successDurable() && (failed || facts.terminal() == Terminal.RUNNING)) return SealDecision.REJECT;
        if (facts.executionStopped() && facts.terminal() == Terminal.RUNNING) return SealDecision.REJECT;
        if (!facts.metadataSealed() && facts.receipt() == Receipt.MATCHING) return SealDecision.REJECT;
        // Sealing and completion are one transaction, so disagreement indicates broken durable state.
        if (facts.metadataSealed() != (current.state() == StageState.COMPLETED)) return SealDecision.REJECT;
        if (facts.metadataSealed() && (!facts.successDurable() || facts.evidence() == Evidence.ABSENT
                || facts.evidence() == Evidence.INCOMPLETE || facts.manifest() == Manifest.ABSENT)) return SealDecision.REJECT;
        // An object read error must never become a negative existence result or trigger overwrite/retry.
        if (facts.evidence() == Evidence.UNKNOWN || facts.manifest() == Manifest.UNKNOWN
                || facts.receipt() == Receipt.UNKNOWN || facts.files() == FileCheck.UNKNOWN) return SealDecision.WAIT_UNKNOWN;
        if (!facts.successDurable()) {
            if (facts.terminal() == Terminal.SUCCEEDED) return SealDecision.PERSIST_SUCCESS;
            if (facts.executionStopped() && facts.evidenceDeadlineExpired()) return SealDecision.ABANDON_AND_RETRY;
            return failed || facts.terminal() == Terminal.RUNNING ? SealDecision.WAIT_STOP : SealDecision.WAIT_UNKNOWN;
        }
        if (facts.evidence() != Evidence.COMPLETE) {
            return facts.executionStopped() && facts.evidenceDeadlineExpired()
                    ? SealDecision.ABANDON_AND_RETRY : SealDecision.WAIT_EVIDENCE;
        }
        if (facts.files() == FileCheck.NOT_CHECKED) return SealDecision.VERIFY_FILES;
        if (facts.manifest() == Manifest.ABSENT) return SealDecision.BUILD_MANIFEST;
        if (!facts.metadataSealed()) return SealDecision.SEAL_METADATA;
        return facts.receipt() == Receipt.ABSENT ? SealDecision.REBUILD_RECEIPT : SealDecision.DONE;
    }
}
