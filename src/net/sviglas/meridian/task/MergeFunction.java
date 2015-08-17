package net.sviglas.meridian.task;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 17/08/15.
 */
public abstract class MergeFunction<T> {
    private Class<T> inputType;

    public MergeFunction(Class<T> t) {
        inputType = t;
    }

    public Class<T> getInputType() { return inputType; }

    public abstract int compare(T l, T r);
}
