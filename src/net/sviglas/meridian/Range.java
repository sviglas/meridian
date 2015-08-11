package net.sviglas.meridian;

import java.util.Iterator;

import net.sviglas.util.Pair;

public abstract class Range<T> {
    public abstract boolean smallEnough();
    public abstract Pair<Range<T>, Range<T>> split();
    public abstract Iterator<T> iterator();
    public abstract T begin();
    public abstract T end();
}
