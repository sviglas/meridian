package net.sviglas.meridian.task;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */
public abstract class MapFunction<TIn, TOut> {
    private Class<TIn> inputType;
    private Class<TOut> outputType;

    public MapFunction(Class<TIn> tin, Class<TOut> tout) {
        inputType = tin;
        outputType = tout;
    }

    public Class<TIn> getInputType() { return inputType; }

    public Class<TOut> getOutputType() { return outputType; }

    public abstract TOut map(TIn t);
}
