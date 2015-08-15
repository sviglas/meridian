package net.sviglas.meridian.storage;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 14/08/15.
 */
public class BadAccessException extends RuntimeException {
    public BadAccessException(String m) {
        super(m);
    }

    public BadAccessException(String m, Throwable t) {
        super(m, t);
    }
}
