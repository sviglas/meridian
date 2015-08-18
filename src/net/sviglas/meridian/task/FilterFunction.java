/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

/**
 * Basic abstraction of a filtering function to be used by filtering tasks.
 *
 * @param <T> the input type.
 */
public abstract class FilterFunction<T> {
    // the input type
    private Class<T> inputType;

    /**
     * Constructs a new filtering function given its input type.
     *
     * @param t the input type.
     */
    public FilterFunction(Class<T> t) {
        inputType = t;
    }

    /**
     * Returns the input type.
     *
     * @return the input type.
     */
    public Class<T> getInputType() { return inputType; }

    /**
     * Filtering function, returns true of the filtering predicate is true,
     * false otherwise.
     *
     * @param t the input value.
     * @return true if the filtering predicate is true, false otherwise.
     */
    public abstract boolean filter(T t);
}
