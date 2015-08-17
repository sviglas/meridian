package net.sviglas.meridian.storage;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */
public class BadTypeException extends RuntimeException {
    /**
     * Constructs a new bad type exception.
     * @param m the error message.
     */
    public BadTypeException(String m) {
        super(m);
    }

    /**
     * Constructs a new bad type exception with an originating throwable.
     * @param m the error message.
     * @param e the originating throwable.
     */
    public BadTypeException(String m, Throwable e) {
        super(m, e);
    }
}
