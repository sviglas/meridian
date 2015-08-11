package net.sviglas.meridian;

import net.sviglas.util.Pair;

public interface Extractor <R, K, V> {
    public Pair<K, V> extract(R r);
}
