package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.BadTypeException;
import net.sviglas.meridian.storage.Dataset;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 16/08/15.
 */
public interface DatasetConstructor {
    <T> Dataset<T> constructDataset(Class<T> type) throws BadTypeException;
}
