/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

/**
 * Basic abstraction for a join function.  Given two inputs it computes whether
 * they should be joined, and combines them into a new output.
 *
 * @param <Tl> the left input type.
 * @param <Tr> the right input type.
 * @param <TOut> the output type.
 */
public abstract class JoinFunction<Tl, Tr, TOut> {
    // the left input type
    private Class<Tl> leftInputType;
    // the right input type
    private Class<Tr> rightInputType;
    // the output type
    private Class<TOut> outputType;

    /**
     * Constructs a new join function given input types.
     *
     * @param tl the left input type.
     * @param tr the right input type.
     * @param tout the output type.
     */
    public JoinFunction(Class<Tl> tl, Class<Tr> tr, Class<TOut> tout) {
        leftInputType = tl;
        rightInputType = tr;
        outputType = tout;
    }

    /**
     * Returns the left input type.
     *
     * @return the left input type.
     */
    public Class<Tl> getLeftInputType() { return leftInputType; }

    /**
     * Returns the right input type.
     *
     * @return the right input type.
     */
    public Class<Tr> getRightInputType() { return rightInputType; }

    /**
     * Returns the output type.
     *
     * @return the output type.
     */
    public Class<TOut> getOutputValueType() { return outputType; }

    /**
     * Checks whether two inputs are equal or not.
     *
     * @return true of the two inputs are equal, false otherwise.
     */
    public abstract boolean equal(Tl l, Tr r);

    /**
     * Combines two inputs into an output one.
     *
     * @param l the left input.
     * @param r the right input.
     * @return the left and right inputs combined into an output one.
     */
    public abstract TOut combine(Tl l, Tr r);
}
