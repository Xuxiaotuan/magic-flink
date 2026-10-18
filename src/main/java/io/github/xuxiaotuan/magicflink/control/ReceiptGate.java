package io.github.xuxiaotuan.magicflink.control;

import java.util.Objects;
import java.util.Optional;

/** Pure policy. Caller MUST recheck/CAS inside the transaction that accepts evidence and advances state. */
public final class ReceiptGate {
    private ReceiptGate() {}
    public enum StageState {
        ACTIVE,
        /** Durable stage completion exists for exactly this current identity; no further phase advance. */
        COMPLETED,
        INELIGIBLE
    }
    public record Current(StageIdentity identity, long revision, StageState state,
                          boolean cancelRequested, Optional<String> acceptedReceiptDigest) {
        public Current {
            Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(state, "state");
            Objects.requireNonNull(acceptedReceiptDigest, "acceptedReceiptDigest");
            acceptedReceiptDigest.ifPresent(StageIdentity::requireText);
            if (revision < 0) throw new IllegalArgumentException("negative revision");
        }
    }
    /** evidenceComplete must come from verified durable facts, never the untrusted callback itself. */
    public record StageReceipt(StageIdentity identity, String contentDigest, boolean evidenceComplete) {
        public StageReceipt {
            Objects.requireNonNull(identity, "identity"); StageIdentity.requireText(contentDigest);
        }
    }
    public enum ReceiptDecision { ACCEPT, ALREADY_ACCEPTED, RECORD_ONLY, WAIT_EVIDENCE, ARCHIVE_ONLY, CONFLICT, REREAD }

    public static ReceiptDecision decide(Current current, long observedRevision, StageReceipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        ReceiptDecision qualification = qualify(current, observedRevision, receipt.identity());
        if (qualification != ReceiptDecision.ACCEPT) return qualification;
        if (current.acceptedReceiptDigest().isPresent()) {
            return current.acceptedReceiptDigest().get().equals(receipt.contentDigest())
                    ? ReceiptDecision.ALREADY_ACCEPTED : ReceiptDecision.CONFLICT;
        }
        if (!receipt.evidenceComplete()) return ReceiptDecision.WAIT_EVIDENCE;
        return current.state() == StageState.COMPLETED ? ReceiptDecision.RECORD_ONLY : ReceiptDecision.ACCEPT;
    }

    static ReceiptDecision qualify(Current current, long observedRevision, StageIdentity identity) {
        Objects.requireNonNull(current, "current");
        // Another input can change the run revision. Reread before interpreting eligibility.
        if (current.revision() != observedRevision) return ReceiptDecision.REREAD;
        if (!current.identity().equals(identity) || current.cancelRequested() || current.state() == StageState.INELIGIBLE)
            return ReceiptDecision.ARCHIVE_ONLY;
        return ReceiptDecision.ACCEPT;
    }
}
