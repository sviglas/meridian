package net.sviglas.meridian.task;

import java.util.concurrent.RecursiveTask;

public abstract class Task<E> extends RecursiveTask<E> {
    private DatasetConstructor datasetConstructor;

    public Task() {
        this(new DefaultDatasetConstructor());
    }

    public Task(DatasetConstructor dc) {
        datasetConstructor = dc;
    }

    public DatasetConstructor getDatasetConstructor() {
        return datasetConstructor;
    }
}
