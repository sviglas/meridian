package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

public interface Extractor <R, T> {
    T extract(R r);
}
