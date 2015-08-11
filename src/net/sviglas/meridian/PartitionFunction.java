package net.sviglas.meridian;

import net.sviglas.util.Pair;

public interface PartitionFunction<TIn, KOut, VOut> {
    Pair<KOut, VOut> apply(TIn t);
}
