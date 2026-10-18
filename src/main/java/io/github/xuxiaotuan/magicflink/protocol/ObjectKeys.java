package io.github.xuxiaotuan.magicflink.protocol;

import java.net.URI;
import java.util.Locale;

/** One spelling for system-generated S3 keys, shared by metadata protocols and local storage. */
public final class ObjectKeys {
    private ObjectKeys() {}
    public static URI file(URI key) { return validate(key, false); }
    public static URI prefix(URI key) { return validate(key, true); }
    private static URI validate(URI uri, boolean prefix) {
        if (uri == null || !"s3".equals(uri.getScheme()) || uri.getHost() == null || !uri.getHost().equals(uri.getHost().toLowerCase(Locale.ROOT))
                || uri.getUserInfo() != null || uri.getPort() != -1 || uri.getQuery() != null || uri.getFragment() != null
                || !uri.normalize().equals(uri) || !uri.toASCIIString().equals(uri.toString()) || uri.getPath() == null
                || !uri.getPath().matches("/[A-Za-z0-9_./-]+") || uri.getPath().contains("//") || java.util.Arrays.stream(uri.getPath().split("/")).anyMatch(part -> part.equals(".") || part.equals(".."))
                || uri.getPath().endsWith("/") != prefix)
            throw new IllegalArgumentException("noncanonical internal S3 key");
        return uri;
    }
    public static boolean contains(URI prefix, URI key) {
        prefix(prefix); file(key);
        return prefix.getHost().equals(key.getHost()) && key.getPath().startsWith(prefix.getPath());
    }
}
