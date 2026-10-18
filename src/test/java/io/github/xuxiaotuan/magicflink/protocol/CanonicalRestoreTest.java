package io.github.xuxiaotuan.magicflink.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CanonicalRestoreTest {
    @ParameterizedTest @ValueSource(strings = {"{\"a\":1,\"a\":2}", "{\"b\":1,\"a\":2}", "{ \"a\":1}", "{\"a\":1.2}", "{\"a\":null}", "a: 1", "!!java.lang.String x", "[&x [1], *x]"})
    void rejectsNoncanonicalOrUnsafeInput(String input) {
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.decode(input.getBytes(StandardCharsets.UTF_8)));
    }
    @Test void rejectsBadUtf8UnknownProtocolAndMismatchedDigest() {
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.decode(new byte[]{(byte)0xff}));
        byte[] unknown = CanonicalJson.encode(Map.of("protocol", "unapproved", "kind", "SCHEMA", "content", Map.of("x", 1)));
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.restoreCanonical(unknown, CanonicalJson.sha256(unknown)));
        var value = DomainSnapshots.catalog(FileStageProtocolTest.catalog());
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.restoreCanonical(value.canonicalBytes(), "a".repeat(64)));
        var restored = ObjectDigests.restoreCanonical(value.canonicalBytes(), value.sha256());
        byte[] bytes = restored.canonicalBytes(); bytes[0] = 0;
        assertArrayEquals(value.canonicalBytes(), restored.canonicalBytes());
        var map = (Map<?, ?>)CanonicalJson.decode(value.canonicalBytes());
        assertThrows(UnsupportedOperationException.class, map::clear);
    }
}
