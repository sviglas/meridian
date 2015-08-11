package net.sviglas.meridian;

/**
 * Created by sviglas on 11/08/15.
 */
public interface FoldFunction<TIn, TOut> {
    TOut noop();
    TOut apply(TOut in, TIn n);
    TOut combine(TOut l, TOut r);
}
