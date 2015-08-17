package net.sviglas.meridian.task;

public abstract class FilterFunction<T> {
    private Class<T> inputType;

    public FilterFunction(Class<T> t) {
        inputType = t;
    }

    public Class<T> getInputType() { return inputType; }

    public abstract boolean filter(T t);
}
