package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.protocol.ObjectDigests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class LocalImmutableObjectStoreTest {
    @TempDir Path root;
    static final URI SCOPE = URI.create("s3://test-bucket/magic/");
    static final URI KEY = URI.create(SCOPE + "runs/run1/manifest.json");
    static ObjectDigests.Frozen payload(String type) {
        return ObjectDigests.freeze(ObjectDigests.Kind.SCHEMA, Map.of("columns", List.of(Map.of("name", "id", "type", type))));
    }
    Path objectPath() { return root.resolve("test-bucket/magic/runs/run1/manifest.json"); }

    @Test void createsReadsAndReopensImmutableSnapshot() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE); var document = payload("BIGINT");
        assertEquals(LocalImmutableObjectStore.PutResult.CREATED, store.putIfAbsent(KEY, document));
        assertEquals(LocalImmutableObjectStore.PutResult.ALREADY_EXISTS, store.putIfAbsent(KEY, document));
        var reopened = new LocalImmutableObjectStore(root, SCOPE).get(KEY, document.sha256()).orElseThrow();
        assertArrayEquals(document.canonicalBytes(), reopened.canonicalBytes());
        assertTrue(store.get(URI.create(SCOPE + "missing.json"), document.sha256()).isEmpty());
        assertEquals(PosixFilePermissions.fromString("r--------"), Files.getPosixFilePermissions(objectPath()));
    }

    @Test void conflictNeverOverwritesExistingBytes() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE); var original = payload("BIGINT");
        store.putIfAbsent(KEY, original);
        assertThrows(LocalImmutableObjectStore.ConflictException.class, () -> store.putIfAbsent(KEY, payload("STRING")));
        assertArrayEquals(original.canonicalBytes(), store.get(KEY, original.sha256()).orElseThrow().canonicalBytes());
    }

    @Test void failureBeforePublishLeavesNoVisiblePartialObject() throws Exception {
        var failing = new LocalImmutableObjectStore(root, SCOPE, point -> {
            if (point == LocalImmutableObjectStore.FaultPoint.BEFORE_LINK) throw new IOException("injected before publication");
        });
        assertThrows(IOException.class, () -> failing.putIfAbsent(KEY, payload("BIGINT")));
        assertTrue(new LocalImmutableObjectStore(root, SCOPE).get(KEY, payload("BIGINT").sha256()).isEmpty());
    }

    @Test void lostResponseAfterPublishIsRecoveredBySameContentRetry() throws Exception {
        var failing = new LocalImmutableObjectStore(root, SCOPE, point -> {
            if (point == LocalImmutableObjectStore.FaultPoint.AFTER_LINK) throw new IOException("injected lost acknowledgement");
        });
        assertThrows(IOException.class, () -> failing.putIfAbsent(KEY, payload("BIGINT")));
        var recovered = new LocalImmutableObjectStore(root, SCOPE);
        assertEquals(LocalImmutableObjectStore.PutResult.ALREADY_EXISTS, recovered.putIfAbsent(KEY, payload("BIGINT")));
        assertArrayEquals(payload("BIGINT").canonicalBytes(), recovered.get(KEY, payload("BIGINT").sha256()).orElseThrow().canonicalBytes());
    }

    @Test void corruptionAndReadErrorsAreNotAbsence() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE); var value = payload("BIGINT"); store.putIfAbsent(KEY, value);
        Files.setPosixFilePermissions(objectPath(), PosixFilePermissions.fromString("rw-------"));
        Files.writeString(objectPath(), "damaged");
        assertThrows(LocalImmutableObjectStore.CorruptObjectException.class, () -> store.get(KEY, value.sha256()));
        URI directoryKey = URI.create(SCOPE + "not-a-file.json");
        Files.createDirectory(root.resolve("test-bucket/magic/not-a-file.json"));
        assertThrows(IOException.class, () -> store.get(directoryKey, value.sha256()));
    }

    @Test void rejectsSymlinkParents() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE); var value = payload("BIGINT");
        Path outside = Files.createDirectory(root.resolve("outside"));
        Files.createDirectory(root.resolve("test-bucket"), PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Files.createSymbolicLink(root.resolve("test-bucket/magic"), outside);
        assertThrows(IOException.class, () -> store.putIfAbsent(KEY, value));
        assertThrows(IOException.class, () -> store.get(KEY, value.sha256()));
        assertFalse(Files.exists(outside.resolve("runs")));
    }

    @Test void rejectsSymlinkObjectWithoutFollowingIt() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE); var value = payload("BIGINT");
        store.putIfAbsent(KEY, value);
        Path outside = root.resolve("outside.json"); Files.write(outside, value.canonicalBytes());
        URI link = URI.create(SCOPE + "runs/run1/link.json");
        Files.createSymbolicLink(objectPath().resolveSibling("link.json"), outside);
        assertThrows(IOException.class, () -> store.get(link, value.sha256()));
        assertThrows(IOException.class, () -> store.putIfAbsent(link, value));
        assertArrayEquals(value.canonicalBytes(), Files.readAllBytes(outside));
    }

    @ParameterizedTest @ValueSource(strings = {"s3://other-bucket/magic/a.json", "s3://test-bucket/magic_evil/a.json", "s3://test-bucket/magic/../a.json", "s3://test-bucket/magic/a.json?q=secret"})
    void rejectsOutOfScopeAndNoncanonicalKeys(String key) throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE);
        assertThrows(IllegalArgumentException.class, () -> store.putIfAbsent(URI.create(key), payload("BIGINT")));
    }

    @Test void concurrentInstancesPublishExactlyOneValue() throws Exception {
        var store = new LocalImmutableObjectStore(root, SCOPE);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Callable<String>> actions = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                final String type = i % 2 == 0 ? "BIGINT" : "STRING";
                actions.add(() -> {
                    try { return new LocalImmutableObjectStore(root, SCOPE).putIfAbsent(KEY, payload(type)).name() + ":" + type; }
                    catch (LocalImmutableObjectStore.ConflictException e) { return "CONFLICT"; }
                });
            }
            var results = pool.invokeAll(actions); List<String> values = new ArrayList<>();
            for (var result : results) values.add(result.get());
            assertEquals(1, values.stream().filter(v -> v.startsWith("CREATED")).count());
            String winner = values.stream().filter(v -> v.startsWith("CREATED")).findFirst().orElseThrow().split(":")[1];
            assertArrayEquals(payload(winner).canonicalBytes(), store.get(KEY, payload(winner).sha256()).orElseThrow().canonicalBytes());
        } finally { pool.shutdownNow(); }
    }

    @Test void separateJvmProcessesRaceOnTheSameKey() throws Exception {
        List<Process> children = new ArrayList<>();
        try {
            for (String type : List.of("BIGINT", "STRING")) {
                children.add(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx64m", "-XX:ActiveProcessorCount=1",
                        "-cp", System.getProperty("java.class.path"), Worker.class.getName(), root.toString(), type).redirectErrorStream(true).start());
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while ((!Files.exists(root.resolve("ready-BIGINT")) || !Files.exists(root.resolve("ready-STRING"))) && System.nanoTime() < deadline) Thread.sleep(10);
            assertTrue(Files.exists(root.resolve("ready-BIGINT")) && Files.exists(root.resolve("ready-STRING")));
            Files.createFile(root.resolve("go"));
            List<String> results = new ArrayList<>();
            for (var child : children) {
                assertTrue(child.waitFor(15, TimeUnit.SECONDS));
                String output = new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
                assertEquals(0, child.exitValue(), output); results.add(output);
            }
            assertEquals(1, results.stream().filter(v -> v.startsWith("CREATED")).count());
            assertEquals(1, results.stream().filter(v -> v.equals("CONFLICT")).count());
        } finally { children.forEach(p -> { if (p.isAlive()) p.destroyForcibly(); }); }
    }
    public static class Worker {
        public static void main(String[] args) throws Exception {
            Path root = Path.of(args[0]); String type = args[1]; var store = new LocalImmutableObjectStore(root, SCOPE);
            Files.createFile(root.resolve("ready-" + type));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (!Files.exists(root.resolve("go")) && System.nanoTime() < deadline) Thread.sleep(10);
            if (!Files.exists(root.resolve("go"))) throw new IOException("barrier timeout");
            try { System.out.println(store.putIfAbsent(KEY, payload(type)).name() + ":" + type); }
            catch (LocalImmutableObjectStore.ConflictException e) { System.out.println("CONFLICT"); }
        }
    }
}
