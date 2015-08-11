package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

public interface PartitionFunction<TIn,
        KOut extends Comparable<? super KOut>, VOut> {
    Pair<KOut, VOut> apply(TIn t);
}
