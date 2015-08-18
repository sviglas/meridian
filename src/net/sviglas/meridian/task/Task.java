/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 16/08/15.
 */

package net.sviglas.meridian.task;

import java.util.concurrent.RecursiveTask;

/**
 * Abstract base class of all tasks of the in-memory analytics runtime.
 *
 * @param <T> the type of elements this task produces.
 */

public abstract class Task<T> extends RecursiveTask<T> {
    // the dataset constuctor for this task.
    private DatasetConstructor datasetConstructor;

    /**
     * Construct a new task with the default dataset constructor.
     */
    public Task() {
        this(new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new task given its dataset constructor.
     *
     * @param dc the dataset constructor.
     */
    public Task(DatasetConstructor dc) {
        datasetConstructor = dc;
    }

    /**
     * Retrieves this task's dataset constructor.
     *
     * @return this task's dataset constructor.
     */
    public DatasetConstructor getDatasetConstructor() {
        return datasetConstructor;
    }
}
