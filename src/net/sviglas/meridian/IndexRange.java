package net.sviglas.meridian;

import java.util.Iterator;

import net.sviglas.util.Pair;

public class IndexRange extends Range<Integer> {    
    private int low;
    private int high;
    private int threshold;

    public IndexRange(int l, int h) {
        this(l, h, 1);
    }

    public IndexRange(int l, int h, int t) {
        low = l;
        high = h;
        threshold = t;
    }

    public boolean smallEnough() {
        return high - low <= threshold;
    }

    public Pair<Range<Integer>, Range<Integer>> split() {
        int mid = (low + high) / 2;
        return new Pair<Range<Integer>, Range<Integer>>(new IndexRange(low, mid, threshold),
                                                        new IndexRange(mid, high, threshold));
    }

    public Iterator<Integer> iterator() {
        return new IndexIterator(low, high);
    }

    public Integer begin() {
        return low;
    }

    public Integer end() {
        return high;
    }

    public class IndexIterator implements Iterator<Integer> {
        private int low;
        private int high;
        private int current;
        public IndexIterator(int l, int h) { low = l; high = h; current = l; }
        public boolean hasNext() { return current++ < high; }
        public Integer next() { return current; }
        public void remove() {}
    }
}
