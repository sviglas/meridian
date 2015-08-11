package net.sviglas.meridian.task;

import java.util.Iterator;

public interface ReduceFunction <KIn, VIn, VOut> {
    VOut apply(KIn k, Iterator<VIn> v);
}
