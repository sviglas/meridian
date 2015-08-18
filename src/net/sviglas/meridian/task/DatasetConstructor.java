/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 16/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.BadTypeException;
import net.sviglas.meridian.storage.Dataset;

/**
 * Encapsulation of a dataset constructor.
 */
public interface DatasetConstructor {
    /**
     * Given a type, construct a dataset for that type.
     *
     * @param type the dataset type.
     * @param <T> the type of dataset records.
     * @return a dataset for records of the given type.
     * @throws BadTypeException if the type is not good for dataset
     * construction, i.e., if it is not a primitive type or consists only
     * of primitive-typed fields, or it does not have a parameter-less
     * constructor.
     */
    <T> Dataset<T> constructDataset(Class<T> type) throws BadTypeException;
}
