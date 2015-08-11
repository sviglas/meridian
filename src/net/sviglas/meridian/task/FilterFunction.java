package net.sviglas.meridian.task;

public interface FilterFunction<R> {
    boolean apply(R r);
}
