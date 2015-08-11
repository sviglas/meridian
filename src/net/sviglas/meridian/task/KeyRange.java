package net.sviglas.meridian.task;

import java.util.Iterator;
import java.util.SortedSet;

import net.sviglas.util.Pair;

public class KeyRange<K> extends Range<K> {
    private SortedSet<K> set;

    public KeyRange(SortedSet<K> s) {
        set = s;
    }

    public boolean smallEnough() {
        return set.size() == 1;
    }

    public Pair<Range<K>, Range<K>> split() {
        if (! smallEnough()) {
            Iterator<K> it = set.iterator();
            K l = it.next();
            K h = it.next();
            return new Pair<>(new KeyRange<>(set.subSet(l, h)),
                    new KeyRange<>(set.tailSet(h)));
        }
        return new Pair<>(this, this);
    }

    public Iterator<K> iterator() {
        return set.iterator();
    }

    public K begin() {
        return set.first();
    }

    public K end() {
        return set.last();
    }
}