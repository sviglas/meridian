package net.sviglas.meridian;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ForkJoinPool;

import net.sviglas.util.Pair;

public class Identity <I extends Comparable<? super I>>
        implements PartitionFunction<I, I, I>, ReduceFunction<I, I, String> {

    public Pair<I, I> apply(I in) { return new Pair<>(in, in); }
    public String apply(I in, Iterator<I> it) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(in).append("->");
        for (; it.hasNext();) sb.append(it.next()).append("_");
        sb.setLength(sb.length()-1);
        sb.append("]");
        return sb.toString();
    }
}
