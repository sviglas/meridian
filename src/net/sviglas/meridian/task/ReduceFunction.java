/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Iterator;

/**
 * Basic abstraction of a reduce function.  Actual reducers should provide the
 * implementation, whereas the rest of the necessary plumbing is here.
 *
 * @param <KIn> the input key type.
 * @param <VIn> the input value type.
 * @param <VOut> the output value type.
 */
public abstract class ReduceFunction <KIn, VIn, VOut> {
    // the input key type
    private Class<KIn> inputKeyType;
    // the input value type
    private Class<VIn> inputValueType;
    // the output value type
    private Class<VOut> outputValueType;

    /**
     * Constructs a new reducer given its input and output type.
     *
     * @param kin the input key type.
     * @param vin the input value type.
     * @param vout the output value type.
     */
    public ReduceFunction(Class<KIn> kin, Class<VIn> vin, Class<VOut> vout) {
        inputKeyType = kin;
        inputValueType = vin;
        outputValueType = vout;
    }

    /**
     * Returns the input key type.
     *
     * @return the input key type.
     */
    public Class<KIn> getInputKeyType() { return inputKeyType; }

    /**
     * Returns the input value type.
     *
     * @return the input value type.
     */
    public Class<VIn> getInputValueType() { return inputValueType; }

    /**
     * Returns the output valuetype.
     *
     * @return the output value type.
     */
    public Class<VOut> getOutputValueType() { return outputValueType; }

    /**
     * The reduce function: it accepts an iterator of values and reduces it to
     * a single value of the given output type.
     *
     * @param k the key value.
     * @param v the iterator over input values.
     * @return the output value.
     */
    public abstract VOut reduce(KIn k, Iterator<VIn> v);
}
