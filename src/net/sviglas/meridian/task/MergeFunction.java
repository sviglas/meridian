/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 17/08/15.
 */

package net.sviglas.meridian.task;

/**
 * Abstraction for a merging function to be used my merging tasks.
 *
 * @param <T> the type of records to merge.
 */
public abstract class MergeFunction<T> {
    // the input type.
    private Class<T> inputType;

    /**
     * Constructs a merging function for the given type.
     *
     * @param t the input type.
     */
    public MergeFunction(Class<T> t) {
        inputType = t;
    }

    /**
     * Returns the input type.
     *
     * @return the input type.
     */
    public Class<T> getInputType() { return inputType; }

    /**
     * Comparator function: given two elements it returns a negative, zero, or
     * positive value depending on whether the first parameter is smaller,
     * equal, or greater than the second parameter.
     *
     * @param l the left parameter.
     * @param r the right parameter.
     * @return a negative, zero, or positive value depending on the comparison
     * between the two parameters (resp. smaller, equal, greater).
     */
    public abstract int compare(T l, T r);
}
