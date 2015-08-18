/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 14/08/15.
 */

package net.sviglas.meridian.storage;

/**
 * Signifies a bad access to a dataset.
 */
public class BadAccessException extends RuntimeException {
    /**
     * Constructs a new exception given its message.
     *
     * @param m the error message.
     */
    public BadAccessException(String m) {
        super(m);
    }

    /**
     * Constructs a new exception given its message and originating throwable.
     *
     * @param m the error message.
     * @param t the originating throwable.
     */
    public BadAccessException(String m, Throwable t) {
        super(m, t);
    }
}
