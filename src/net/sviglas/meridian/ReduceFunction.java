package net.sviglas.meridian;

import java.util.Iterator;

public interface ReduceFunction <KIn, VIn, VOut> {
    public VOut apply(KIn k, Iterator<VIn> v);
}
