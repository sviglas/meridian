/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

/**
 * Abstraction for a mapping function to be used my mapping tasks.
 *
 * @param <TIn> the input type.
 * @param <TOut> the output type.
 */
public abstract class MapFunction<TIn, TOut> {
    // the input type
    private Class<TIn> inputType;
    // the output type
    private Class<TOut> outputType;

    /**
     * Constructs a mapping function for the given types.
     *
     * @param tin the input type.
     * @param tout the output type.
     */
    public MapFunction(Class<TIn> tin, Class<TOut> tout) {
        inputType = tin;
        outputType = tout;
    }

    /**
     * Returns the input type.
     *
     * @return the input type.
     */
    public Class<TIn> getInputType() { return inputType; }

    /**
     * Returns the output type.
     *
     * @return the output type.
     */
    public Class<TOut> getOutputType() { return outputType; }

    /**
     * Maps an input value to an output one.
     *
     * @param t the input value.
     * @return the mapping function applied on the input.
     */
    public abstract TOut map(TIn t);
}
