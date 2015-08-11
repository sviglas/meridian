package net.sviglas.meridian.task;

/**
 * Created by sviglas on 10/08/15.
 */
public interface MapFunction<TIn, TOut> {
    TOut apply(TIn t);
}
