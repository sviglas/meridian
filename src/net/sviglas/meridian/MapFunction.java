package net.sviglas.meridian;

/**
 * Created by sviglas on 10/08/15.
 */
public interface MapFunction<TIn, TOut> {
    TOut apply(TIn t);
}
