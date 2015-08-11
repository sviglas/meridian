package net.sviglas.meridian.task;

/**
 * Created by sviglas on 10/08/15.
 */
public interface JoinFunction<Tl, Tr, TOut> {
    boolean areEqual(Tl l, Tr r);
    TOut combine(Tl l, Tr r);
}
