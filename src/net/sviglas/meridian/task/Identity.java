package net.sviglas.meridian.task;

import java.util.Iterator;

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
