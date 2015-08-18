/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 17/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.ArrayStore;
import net.sviglas.meridian.storage.BadTypeException;
import net.sviglas.meridian.storage.Dataset;

/**
 * Default dataset constructor; returns an array store.
 */
public class DefaultDatasetConstructor implements DatasetConstructor {
    /**
     * Given a type, construct a dataset for that type; the default type
     * of dataset is an array store.
     *
     * @param type the dataset type.
     * @param <T> the type of dataset records.
     * @return a dataset for records of the given type.
     * @throws BadTypeException if the type is not good for dataset
     * construction, i.e., if it is not a primitive type or consists only
     * of primitive-typed fields, or it does not have a parameter-less
     * constructor.
     */
    @Override
    public <T> Dataset<T> constructDataset(Class<T> type)
            throws BadTypeException {
        return new ArrayStore<>(type);
    }
}
