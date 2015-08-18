/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Iterator;

import net.sviglas.util.Pair;

/**
 * Key abstraction of a range.
 *
 * @param <T> the enumerable indexes of the range.
 */
public abstract class Range<T> {
    /**
     * Returns whether this range is small enough for processing.
     *
     * @return true if the range is small enough for processing by a single
     * thread, false otherwise.
     */
    public abstract boolean smallEnough();

    /**
     * Splits this range into two new smaller ranges.
     *
     * @return a pair of new smaller ranges.
     */
    public abstract Pair<Range<T>, Range<T>> split();

    /**
     * Returns an iterator over the elements of this range.
     *
     * @return an iterator over the elements of this range.
     */
    public abstract Iterator<T> iterator();

    /**
     * Returns the beginning of this range.
     *
     * @return the beginning of this range.
     */
    public abstract T begin();

    /**
     * Returns the end of this range.
     *
     * @return the end of this range.
     */
    public abstract T end();
}
