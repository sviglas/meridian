/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

/**
 * Encapsulation of a partitioning function for partitioning workflows.
 *
 * @param <TIn> the input type.
 * @param <KOut> the output key type.
 * @param <VOut> the output value type.
 */
public abstract class PartitionFunction<TIn,
        KOut extends Comparable<? super KOut>, VOut> {
    // the input type
    private Class<TIn> inputType;
    // the output key type
    private Class<KOut> outputKeyType;
    // the output value type.
    private Class<VOut> outputValueType;

    /**
     * Constructs a new partitioning function given input/output types.
     *
     * @param tin the input type.
     * @param kout the output key type.
     * @param vout the output value type.
     */
    public PartitionFunction(Class<TIn> tin, Class<KOut> kout,
                             Class<VOut> vout) {
        inputType = tin;
        outputKeyType = kout;
        outputValueType = vout;
    }

    /**
     * Returns the input type.
     *
     * @return the input type.
     */
    public Class<TIn> getInputType() { return inputType; }

    /**
     * Returns the output key type.
     *
     * @return the output key type.
     */
    public Class<KOut> getOutputKeyType() { return outputKeyType; }

    /**
     * Returns the output value type.
     *
     * @return the output values type.
     */
    public Class<VOut> getOutputValueType() { return outputValueType; }

    /**
     * The partitioning function: given an input it converts it into a key-value
     * pair to be consumed by the partitioning task.
     *
     * @param t the input.
     * @return a key-value pair.
     */
    public abstract Pair<KOut, VOut> partition(TIn t);
}
