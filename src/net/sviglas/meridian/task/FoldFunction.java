/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

/**
 * Basic encapsulation of a functional fold operation.
 *
 * @param <TIn> the input type.
 * @param <TOut> the output type.
 */
public abstract class FoldFunction<TIn, TOut> {
    // the input type
    private Class<TIn> inputType;
    // the output type
    private Class<TOut> outputType;

    /**
     * Constructs a new fold function for the given types.
     *
     * @param tin the input type.
     * @param tout the output type.
     */
    public FoldFunction(Class<TIn> tin, Class<TOut> tout) {
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
     * The no-operation; basically the neutral value of the fold.
     *
     * @return the neutral value of the fold.
     */
    public abstract TOut noop();

    /**
     * The accumulator for the fold; given a formed output, accumulates an input
     * to it.
     *
     * @param in the accumulated value.
     * @param n the input value.
     * @return the new output value.
     */
    public abstract TOut accumulate(TOut in, TIn n);

    /**
     * Combines two output values into a single one.
     *
     * @param l the left input value.
     * @param r the right input value.
     * @return the combined output.s
     */
    public abstract TOut combine(TOut l, TOut r);
}
