package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import io.github.xuxiaotuan.magicflink.spec.JobSpec.ParameterType;
import org.junit.jupiter.api.Test;
import java.math.*;
import java.time.LocalDate;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocolTest.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcRunStoreTest.*;
import static org.junit.jupiter.api.Assertions.*;

class TransactionContractsTest {
    @Test void physicalBatchIdentityNormalizesDecimalScaleAndRetainsTypeAndTenant() {
        var first=new BusinessKey("bank1","target1",ParameterType.DECIMAL,new BigDecimal("1.00"));
        var second=new BusinessKey("bank1","target1",ParameterType.DECIMAL,new BigDecimal("1.0"));
        assertEquals(first,second);assertEquals(first.digest(),second.digest());assertEquals(first,BusinessKey.restore(first.canonical()));
        assertNotEquals(first.digest(),new BusinessKey("bank1","target1",ParameterType.STRING,"1").digest());
        assertNotEquals(first.digest(),new BusinessKey("bank2","target1",ParameterType.DECIMAL,BigDecimal.ONE).digest());
        assertNotEquals(KEY.digest(),new BusinessKey("bank1","postgres_result",ParameterType.STRING,"2026-10-18").digest());
        assertThrows(IllegalArgumentException.class,()->new BusinessKey("bank1","target1",ParameterType.DATE,"2026-10-18"));
    }
    @Test void requestGoldenVectorBindsAllPublicationFields() {
        var request=new PublicationRequest("pub1","run1",KEY,"load1",HASH,0);
        // Independently calculated with Python json/hashlib; not derived from the Java producer.
        assertEquals("5c94d49a230375f306b613f5f5875cf8ba9abfdb5cb430925f3db25637cb24f3",request.requestDigest());
        assertNotEquals(request.requestDigest(),new PublicationRequest("pub1","run1",KEY,"load2",HASH,0).requestDigest());
        assertNotEquals(request.requestDigest(),new PublicationRequest("pub1","run1",KEY,"load1",HASH,1).requestDigest());
        assertThrows(IllegalArgumentException.class,()->new PublicationRequest("pub1","run1",KEY,"load1",HASH,Long.MAX_VALUE));
    }
    @Test void identityReconstructionBindsResourceAndRejectsExtraOrMissingFields() {
        var i=identity(plan());assertEquals(i,TransactionContracts.restoreIdentity(TransactionContracts.identityCanonical(i)));
        var changed=new StageIdentity(i.stage(),i.attemptId(),i.executor(),Optional.of(new StageIdentity.FlinkResource("another_uid","job1")),i.planDigest(),i.resolvedSpecDigest(),i.consumedDataDigest());
        assertNotEquals(TransactionContracts.identityDigest(i),TransactionContracts.identityDigest(changed));
        String body=TransactionContracts.identityCanonical(i);
        assertThrows(IllegalArgumentException.class,()->TransactionContracts.restoreIdentity(body.replace("\"attempt\":", "\"extra\":1,\"attempt\":")));
        assertThrows(RuntimeException.class,()->TransactionContracts.restoreIdentity("{}"));
    }
    @Test void targetAuthorizationCannotHaveNoRulesOrMixedRunIdentity() {
        var original=readyRegistration();
        assertThrows(IllegalArgumentException.class,()->new Registration(original.runId(),original.resolvedSpecDigest(),original.phase(),original.stages(),original.publication(),Map.of(),original.deadline(),original.retryPolicy(),original.timeLimits()));
        assertThrows(IllegalArgumentException.class,()->new Registration("another_run",original.resolvedSpecDigest(),original.phase(),original.stages(),original.publication(),original.requiredChecks(),original.deadline(),original.retryPolicy(),original.timeLimits()));
        assertThrows(IllegalArgumentException.class,()->new PublicationReceipt(new PublicationRequest("pub1","run1",KEY,"load1",HASH,0),Optional.empty(),2,CLOCK.instant()));
    }
}
