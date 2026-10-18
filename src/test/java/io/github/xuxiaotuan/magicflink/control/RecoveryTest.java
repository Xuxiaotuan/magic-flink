package io.github.xuxiaotuan.magicflink.control;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static io.github.xuxiaotuan.magicflink.control.StageIdentity.*;
import static io.github.xuxiaotuan.magicflink.control.ReceiptGate.*;
import static io.github.xuxiaotuan.magicflink.control.SealRecovery.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryTest {
    private static final StageIdentity ID = identity("orders", "attempt1", "uid1", "job1", "plan", "spec", "data");
    private static StageIdentity identity(String alias, String attempt, String uid, String job, String plan, String spec, String data) {
        return new StageIdentity(new StageKey("run1", StageType.COLLECT, Optional.of(alias)), attempt,
                Executor.FLINK, Optional.of(new FlinkResource(uid, job)), plan, spec, data);
    }
    private Current current(StageState state, boolean cancelled, Optional<String> accepted) {
        return new Current(ID, 7, state, cancelled, accepted);
    }
    private Current active() { return current(StageState.ACTIVE, false, Optional.empty()); }
    private Observation facts(Terminal terminal, boolean durable, Evidence evidence, FileCheck files,
                              Manifest manifest, boolean sealed, Receipt receipt, boolean stopped, boolean expired) {
        return new Observation(ID, terminal, durable, evidence, files, manifest, sealed, receipt, stopped, expired);
    }
    private Observation complete(Manifest manifest, boolean sealed, Receipt receipt) {
        return facts(Terminal.SUCCEEDED, true, Evidence.COMPLETE, FileCheck.VALID, manifest, sealed, receipt, true, false);
    }

    @Test void acceptsOnlyCompleteCurrentReceipt() {
        assertEquals(ReceiptDecision.ACCEPT, ReceiptGate.decide(active(), 7, new StageReceipt(ID, "receipt", true)));
        assertEquals(ReceiptDecision.WAIT_EVIDENCE, ReceiptGate.decide(active(), 7, new StageReceipt(ID, "receipt", false)));
        assertEquals(ReceiptDecision.REREAD, ReceiptGate.decide(active(), 6, new StageReceipt(ID, "receipt", true)));
    }

    @Test void fencesEveryIdentityComponentIncludingParallelInputAndRecreatedCr() {
        StageIdentity[] invalid = {
                identity("customers", "attempt1", "uid1", "job1", "plan", "spec", "data"),
                identity("orders", "attempt0", "uid1", "job1", "plan", "spec", "data"),
                identity("orders", "attempt1", "uid2", "job1", "plan", "spec", "data"),
                identity("orders", "attempt1", "uid1", "job2", "plan", "spec", "data"),
                identity("orders", "attempt1", "uid1", "job1", "changed", "spec", "data"),
                identity("orders", "attempt1", "uid1", "job1", "plan", "changed", "data"),
                identity("orders", "attempt1", "uid1", "job1", "plan", "spec", "changed"),
                new StageIdentity(ID.stage(), ID.attemptId(), Executor.INLINE, Optional.empty(), "plan", "spec", "data")
        };
        for (StageIdentity id : invalid)
            assertEquals(ReceiptDecision.ARCHIVE_ONLY, ReceiptGate.decide(active(), 7, new StageReceipt(id, "receipt", true)));
    }

    @Test void cancellationOrIneligiblePhaseNeverAdvances() {
        assertEquals(ReceiptDecision.ARCHIVE_ONLY, ReceiptGate.decide(current(StageState.ACTIVE, true, Optional.empty()), 7, new StageReceipt(ID, "receipt", true)));
        assertEquals(ReceiptDecision.ARCHIVE_ONLY, ReceiptGate.decide(current(StageState.INELIGIBLE, false, Optional.empty()), 7, new StageReceipt(ID, "receipt", true)));
    }

    @Test void duplicateIsIdempotentButConflictingDigestIsNotOverwritten() {
        var completed = current(StageState.COMPLETED, false, Optional.of("receipt"));
        assertEquals(ReceiptDecision.ALREADY_ACCEPTED, ReceiptGate.decide(completed, 7, new StageReceipt(ID, "receipt", true)));
        assertEquals(ReceiptDecision.CONFLICT, ReceiptGate.decide(completed, 7, new StageReceipt(ID, "different", true)));
        assertEquals(ReceiptDecision.RECORD_ONLY, ReceiptGate.decide(current(StageState.COMPLETED, false, Optional.empty()), 7, new StageReceipt(ID, "receipt", true)));
    }

    @Test void recoversEachSealWindowWithoutRestartingSuccessfulWork() {
        assertEquals(SealDecision.BUILD_MANIFEST, SealRecovery.decide(active(), 7, complete(Manifest.ABSENT, false, Receipt.ABSENT)));
        assertEquals(SealDecision.SEAL_METADATA, SealRecovery.decide(active(), 7, complete(Manifest.MATCHING, false, Receipt.ABSENT)));
        var completed = current(StageState.COMPLETED, false, Optional.empty());
        assertEquals(SealDecision.REBUILD_RECEIPT, SealRecovery.decide(completed, 7, complete(Manifest.MATCHING, true, Receipt.ABSENT)));
        assertEquals(SealDecision.DONE, SealRecovery.decide(completed, 7, complete(Manifest.MATCHING, true, Receipt.MATCHING)));
    }

    @Test void unknownTerminalCannotBePromotedFromFilesAlone() {
        var facts = facts(Terminal.UNKNOWN, false, Evidence.COMPLETE, FileCheck.VALID, Manifest.MATCHING, false, Receipt.ABSENT, false, true);
        assertEquals(SealDecision.WAIT_UNKNOWN, SealRecovery.decide(active(), 7, facts));
        assertEquals(SealDecision.ABANDON_AND_RETRY, SealRecovery.decide(active(), 7,
                facts(Terminal.UNKNOWN, false, Evidence.COMPLETE, FileCheck.VALID, Manifest.ABSENT, false, Receipt.ABSENT, true, true)));
    }

    @Test void durableSuccessSurvivesLossOfLiveCr() {
        assertEquals(SealDecision.BUILD_MANIFEST, SealRecovery.decide(active(), 7,
                facts(Terminal.UNKNOWN, true, Evidence.COMPLETE, FileCheck.VALID, Manifest.ABSENT, false, Receipt.ABSENT, false, false)));
    }

    @Test void successMustBePersistedBeforeVerification() {
        assertEquals(SealDecision.PERSIST_SUCCESS, SealRecovery.decide(active(), 7,
                facts(Terminal.SUCCEEDED, false, Evidence.COMPLETE, FileCheck.NOT_CHECKED, Manifest.ABSENT, false, Receipt.ABSENT, true, false)));
        assertEquals(SealDecision.VERIFY_FILES, SealRecovery.decide(active(), 7,
                facts(Terminal.SUCCEEDED, true, Evidence.COMPLETE, FileCheck.NOT_CHECKED, Manifest.ABSENT, false, Receipt.ABSENT, true, false)));
    }

    @ParameterizedTest @EnumSource(value = Evidence.class, names = {"ABSENT", "INCOMPLETE"})
    void missingEvidenceRequiresBothDeadlineAndConfirmedStop(Evidence evidence) {
        for (boolean stopped : new boolean[]{false, true}) for (boolean expired : new boolean[]{false, true}) {
            var observation = facts(Terminal.SUCCEEDED, true, evidence, FileCheck.NOT_CHECKED, Manifest.ABSENT, false, Receipt.ABSENT, stopped, expired);
            assertEquals(stopped && expired ? SealDecision.ABANDON_AND_RETRY : SealDecision.WAIT_EVIDENCE,
                    SealRecovery.decide(active(), 7, observation));
        }
    }

    @Test void storageReadFailureIsNotAbsenceEvenAfterDeadline() {
        assertEquals(SealDecision.WAIT_UNKNOWN, SealRecovery.decide(active(), 7,
                facts(Terminal.SUCCEEDED, true, Evidence.UNKNOWN, FileCheck.NOT_CHECKED, Manifest.ABSENT, false, Receipt.ABSENT, true, true)));
        assertEquals(SealDecision.WAIT_UNKNOWN, SealRecovery.decide(active(), 7, complete(Manifest.UNKNOWN, false, Receipt.ABSENT)));
        assertEquals(SealDecision.WAIT_UNKNOWN, SealRecovery.decide(current(StageState.COMPLETED, false, Optional.empty()), 7, complete(Manifest.MATCHING, true, Receipt.UNKNOWN)));
    }

    @Test void conflictsAndCorruptSealedFilesAreNeverRepairedInPlace() {
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7, complete(Manifest.CONFLICT, false, Receipt.ABSENT)));
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7, complete(Manifest.MATCHING, false, Receipt.CONFLICT)));
        assertEquals(SealDecision.REJECT, SealRecovery.decide(current(StageState.COMPLETED, false, Optional.empty()), 7,
                facts(Terminal.UNKNOWN, true, Evidence.COMPLETE, FileCheck.CORRUPT, Manifest.MATCHING, true, Receipt.ABSENT, true, true)));
    }

    @Test void failedJobAndCancelledOrStaleStageCannotSealEvenWithCompleteEvidence() {
        assertEquals(SealDecision.ABANDON_AND_RETRY, SealRecovery.decide(active(), 7,
                facts(Terminal.FAILED, false, Evidence.COMPLETE, FileCheck.VALID, Manifest.ABSENT, false, Receipt.ABSENT, true, true)));
        assertEquals(SealDecision.ARCHIVE_ONLY, SealRecovery.decide(current(StageState.ACTIVE, true, Optional.empty()), 7, complete(Manifest.ABSENT, false, Receipt.ABSENT)));
        assertEquals(SealDecision.REREAD, SealRecovery.decide(active(), 6, complete(Manifest.ABSENT, false, Receipt.ABSENT)));
    }

    @Test void contradictoryFactsFailClosed() {
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7, complete(Manifest.MATCHING, true, Receipt.ABSENT)));
        assertEquals(SealDecision.REJECT, SealRecovery.decide(current(StageState.COMPLETED, false, Optional.empty()), 7, complete(Manifest.ABSENT, true, Receipt.ABSENT)));
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7,
                facts(Terminal.FAILED, true, Evidence.COMPLETE, FileCheck.VALID, Manifest.MATCHING, false, Receipt.ABSENT, true, true)));
    }

    @Test void liveRunningAndStoppedCannotBothAuthorizeRetry() {
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7,
                facts(Terminal.RUNNING, false, Evidence.ABSENT, FileCheck.NOT_CHECKED, Manifest.ABSENT, false, Receipt.ABSENT, true, true)));
    }

    @Test void receiptCannotPrecedeDurableSeal() {
        assertEquals(SealDecision.REJECT, SealRecovery.decide(active(), 7, complete(Manifest.MATCHING, false, Receipt.MATCHING)));
    }

    @Test void fileRecoveryCannotBeUsedForInlineChecksOrTargetWrites() {
        for (StageIdentity other : new StageIdentity[]{
                new StageIdentity(ID.stage(), ID.attemptId(), Executor.INLINE, Optional.empty(), "plan", "spec", "data"),
                new StageIdentity(new StageKey("run1", StageType.WRITE, Optional.empty()), ID.attemptId(), Executor.FLINK, ID.resource(), "plan", "spec", "data")}) {
            var snapshot = new Current(other, 7, StageState.ACTIVE, false, Optional.empty());
            var observation = new Observation(other, Terminal.SUCCEEDED, true, Evidence.COMPLETE, FileCheck.VALID, Manifest.ABSENT, false, Receipt.ABSENT, true, false);
            assertEquals(SealDecision.REJECT, SealRecovery.decide(snapshot, 7, observation));
        }
    }

    @Test void everyRecoveryCombinationPreservesSealAndRetrySafety() {
        for (Terminal terminal : Terminal.values()) for (boolean durable : new boolean[]{false, true})
        for (Evidence evidence : Evidence.values()) for (FileCheck files : FileCheck.values())
        for (Manifest manifest : Manifest.values()) for (Receipt receipt : Receipt.values())
        for (boolean stopped : new boolean[]{false, true}) for (boolean expired : new boolean[]{false, true})
        for (boolean sealed : new boolean[]{false, true}) {
            var observation = facts(terminal, durable, evidence, files, manifest, sealed, receipt, stopped, expired);
            var decision = SealRecovery.decide(current(sealed ? StageState.COMPLETED : StageState.ACTIVE, false, Optional.empty()), 7, observation);
            if (decision == SealDecision.SEAL_METADATA || decision == SealDecision.REBUILD_RECEIPT || decision == SealDecision.DONE) {
                assertTrue(durable && evidence == Evidence.COMPLETE && files == FileCheck.VALID && manifest == Manifest.MATCHING, observation.toString());
                assertTrue(terminal == Terminal.UNKNOWN || terminal == Terminal.SUCCEEDED, observation.toString());
            }
            if (decision == SealDecision.ABANDON_AND_RETRY) {
                assertTrue(stopped && expired && !sealed && terminal != Terminal.RUNNING, observation.toString());
                assertTrue(evidence != Evidence.UNKNOWN && manifest != Manifest.UNKNOWN && receipt != Receipt.UNKNOWN && files != FileCheck.UNKNOWN, observation.toString());
            }
        }
    }
}
