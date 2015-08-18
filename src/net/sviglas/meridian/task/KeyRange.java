/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Iterator;
import java.util.SortedSet;

import net.sviglas.util.Pair;

/**
 * A basic key range enumeration.
 *
 * @param <K> the input key type.
 */
public class KeyRange<K> extends Range<K> {
    // the set of keys
    private SortedSet<K> set;

    /**
     * Constructs a new key range out of a sorted set.
     *
     * @param s the input sorted set.
     */
    public KeyRange(SortedSet<K> s) {
        set = s;
    }

    /**
     * Checks whether the range is small enough to be processed by a single
     * task.
     *
     * @return true of the range is small enough, false otherwise.
     */
    public boolean smallEnough() {
        return set.size() == 1;
    }

    /**
     * Splits this range in two new (smaller) ranges.
     *
     * @return a pair of ranges.
     */
    public Pair<Range<K>, Range<K>> split() {
        // not sure about this...
        if (! smallEnough()) {
            Iterator<K> it = set.iterator();
            K l = it.next();
            K h = it.next();
            return new Pair<>(new KeyRange<>(set.subSet(l, h)),
                    new KeyRange<>(set.tailSet(h)));
        }
        return new Pair<>(this, this);
    }

    /**
     * Returns an iterator over this keyed range.
     *
     * @return an iterator over this keyed range.
     */
    public Iterator<K> iterator() {
        return set.iterator();
    }

    /**
     * The beginning of this range.
     *
     * @return the beginning of this range.
     */
    public K begin() {
        return set.first();
    }

    /**
     * The end of this range.
     *
     * @return the end of this range.
     */
    public K end() {
        return set.last();
    }
}
