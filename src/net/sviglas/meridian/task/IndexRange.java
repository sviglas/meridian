package net.sviglas.meridian.task;

import java.util.Iterator;

import net.sviglas.util.Pair;

public class IndexRange extends Range<Long> {
    private long low;
    private long high;
    private long threshold;

    public IndexRange(long l, long h) {
        this(l, h, 1);
    }

    public IndexRange(long l, long h, long t) {
        low = l;
        high = h;
        threshold = t;
    }

    public boolean smallEnough() {
        return high - low <= threshold;
    }

    public Pair<Range<Long>, Range<Long>> split() {
        long mid = (low + high) / 2;
        return new Pair<>(new IndexRange(low, mid, threshold),
                new IndexRange(mid, high, threshold));
    }

    public Iterator<Long> iterator() {
        return new IndexIterator(low, high);
    }

    public Long begin() {
        return low;
    }

    public Long end() {
        return high;
    }

    public class IndexIterator implements Iterator<Long> {
        private long low;
        private long high;
        private long current;
        public IndexIterator(long l, long h) { low = l; high = h; current = l; }
        public boolean hasNext() { return current++ < high; }
        public Long next() { return current; }
        public void remove() {}
    }
}
