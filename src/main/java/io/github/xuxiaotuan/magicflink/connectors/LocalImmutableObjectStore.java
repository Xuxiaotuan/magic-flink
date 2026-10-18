package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.protocol.ObjectDigests;
import io.github.xuxiaotuan.magicflink.protocol.ObjectKeys;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Optional;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardOpenOption.*;

/** Bounded metadata objects on a private local POSIX filesystem; not an S3 implementation or a database. */
public final class LocalImmutableObjectStore {
    private static final int MAX_BYTES = 1024 * 1024;
    private final Path root, pending;
    private final URI scope;
    private final FaultHook faults;
    public enum PutResult { CREATED, ALREADY_EXISTS }
    public static final class ConflictException extends IOException { public ConflictException() { super("immutable key already contains different content"); } }
    public static final class CorruptObjectException extends IOException { public CorruptObjectException() { super("immutable content does not match its trusted digest/protocol"); } }
    enum FaultPoint { BEFORE_LINK, AFTER_LINK }
    @FunctionalInterface interface FaultHook { void at(FaultPoint point) throws IOException; }

    public LocalImmutableObjectStore(Path root, URI scope) throws IOException { this(root, scope, point -> {}); }
    LocalImmutableObjectStore(Path root, URI scope, FaultHook faults) throws IOException {
        this.scope = ObjectKeys.prefix(scope); this.faults = java.util.Objects.requireNonNull(faults);
        Path absolute = root.toAbsolutePath().normalize(); ensureDirectory(absolute); this.root = absolute.toRealPath();
        pending = this.root.resolve(".pending"); ensureDirectory(pending);
    }
    public PutResult putIfAbsent(URI key, ObjectDigests.Frozen object) throws IOException {
        Path target = path(key, true); ensureDirectory(pending);
        byte[] bytes = object.canonicalBytes();
        if (bytes.length == 0 || bytes.length > MAX_BYTES) throw new IllegalArgumentException("metadata object size invalid");
        Path temporary = Files.createTempFile(pending, "object-", ".tmp", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        try {
            try (var channel = FileChannel.open(temporary, WRITE, NOFOLLOW_LINKS)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes); while (buffer.hasRemaining()) channel.write(buffer); channel.force(true);
            }
            Files.setPosixFilePermissions(temporary, PosixFilePermissions.fromString("r--------"));
            faults.at(FaultPoint.BEFORE_LINK);
            try {
                // Local POSIX link creation publishes the complete inode and refuses an existing name.
                Files.createLink(target, temporary);
            } catch (FileAlreadyExistsException exists) {
                if (!Arrays.equals(bytes, read(target))) throw new ConflictException();
                forceDirectory(target.getParent()); return PutResult.ALREADY_EXISTS;
            } catch (UnsupportedOperationException unsupported) {
                throw new IOException("filesystem lacks no-replace hard-link publication", unsupported);
            }
            faults.at(FaultPoint.AFTER_LINK);
            forceDirectory(target.getParent());
            return PutResult.CREATED;
        } finally {
            // Only this call's unique temporary file is removed. Published objects are never deleted.
            Files.deleteIfExists(temporary);
        }
    }
    public Optional<ObjectDigests.Frozen> get(URI key, String expectedDigest) throws IOException {
        if (expectedDigest == null || !expectedDigest.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("trusted SHA-256 required");
        try {
            byte[] bytes = read(path(key, false));
            try { return Optional.of(ObjectDigests.restoreCanonical(bytes, expectedDigest)); }
            catch (IllegalArgumentException corrupt) { throw new CorruptObjectException(); }
        } catch (NoSuchFileException absent) { return Optional.empty(); }
    }
    private Path path(URI key, boolean createParents) throws IOException {
        if (!ObjectKeys.contains(scope, key)) throw new IllegalArgumentException("object key outside approved storage scope");
        checkDirectory(root);
        Path relative = Path.of(key.getHost()).resolve(key.getPath().substring(1));
        Path target = root.resolve(relative).normalize();
        if (!target.startsWith(root)) throw new IllegalArgumentException("path outside private root");
        Path current = root;
        for (Path component : root.relativize(target.getParent())) {
            current = current.resolve(component);
            if (createParents) ensureDirectory(current); else checkDirectory(current);
        }
        return target;
    }
    private static byte[] read(Path file) throws IOException {
        var attributes = Files.readAttributes(file, BasicFileAttributes.class, NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.size() > MAX_BYTES) throw new IOException("invalid metadata object type/size");
        try (var channel = FileChannel.open(file, READ, NOFOLLOW_LINKS); var bytes = new ByteArrayOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(8192); int count;
            while ((count = channel.read(buffer)) != -1) {
                if (bytes.size() + count > MAX_BYTES) throw new IOException("metadata grew beyond bounded read size");
                bytes.write(buffer.array(), 0, count); buffer.clear();
            }
            return bytes.toByteArray();
        }
    }
    private static void ensureDirectory(Path directory) throws IOException {
        try {
            Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            if (directory.getParent() != null) forceDirectory(directory.getParent());
        } catch (FileAlreadyExistsException exists) { /* Verify type and permissions below, never follow a symlink. */ }
        checkDirectory(directory);
    }
    private static void checkDirectory(Path directory) throws IOException {
        if (!Files.readAttributes(directory, BasicFileAttributes.class, NOFOLLOW_LINKS).isDirectory()
                || !Files.getPosixFilePermissions(directory, NOFOLLOW_LINKS).equals(PosixFilePermissions.fromString("rwx------")))
            throw new IOException("private nonsymlink directory with mode 0700 required");
    }
    private static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, READ, NOFOLLOW_LINKS)) { channel.force(true); }
    }
}
