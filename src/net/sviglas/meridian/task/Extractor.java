package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

public interface Extractor <R, K, V> {
    Pair<K, V> extract(R r);
}
