/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Iterator;

import net.sviglas.util.Pair;

/**
 * A basic index-based range enumeration.
 */
public class IndexRange extends Range<Long> {
    // low key
    private long low;
    // high key
    private long high;
    // threshold for small enough
    private long threshold;

    /**
     * Constructs a new index range for the given low and high key values.
     *
     * @param l the low value.
     * @param h the high value.
     */
    public IndexRange(long l, long h) {
        this(l, h, 1);
    }

    /**
     * Constructs a new index range for the given low and high key values and
     * given threshold.
     *
     * @param l the low value.
     * @param h the high value.
     * @param t the threshold, less than which the range is small enough.
     */
    public IndexRange(long l, long h, long t) {
        low = l;
        high = h;
        threshold = t;
    }

    /**
     * Checks whether the range is small enough to be processed by a single
     * task.
     *
     * @return true of the range is small enough, false otherwise.
     */
    public boolean smallEnough() {
        return high - low <= threshold;
    }

    /**
     * Splits this range in two new (smaller) ranges.
     *
     * @return a pair of ranges.
     */
    public Pair<Range<Long>, Range<Long>> split() {
        long mid = (low + high) / 2;
        return new Pair<>(new IndexRange(low, mid, threshold),
                new IndexRange(mid, high, threshold));
    }

    /**
     * Returns an iterator over this index range.
     *
     * @return an iterator over this index range.
     */
    public Iterator<Long> iterator() {
        return new IndexIterator(low, high);
    }

    /**
     * The beginning of this range.
     *
     * @return the beginning of this range.
     */
    public Long begin() {
        return low;
    }

    /**
     * The end of this range.
     *
     * @return the end of this range.
     */
    public Long end() {
        return high;
    }

    /**
     * Internal iterator class
     */
    class IndexIterator implements Iterator<Long> {
        private long low;
        private long high;
        private long current;
        public IndexIterator(long l, long h) { low = l; high = h; current = l; }
        public boolean hasNext() { return current++ < high; }
        public Long next() { return current; }
        public void remove() {}
    }
}
