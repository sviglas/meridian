/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Basic abstraction of a splitter task.  Splits the dataset in small enough
 * sublists, depending on the given range.
 *
 * @param <T> the records of the dataset.
 */

public class SplitTask<T> extends Task<Dataset<T>> {
    // the input dataset
    private Dataset<T> input;
    // the input record type
    private Class<T> type;
    // the splitting range
    private Range<Long> range;

    /**
     * Constructs a new splitter given its input, record type, and range, with
     * the default dataset constructor.
     *
     * @param i the input dataset.
     * @param c the input record type.
     * @param r the range on which the dataset is split.
     */
    public SplitTask(Dataset<T> i, Class<T> c, Range<Long> r) {
        this(i, c, r, new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new splitter given its input, record type, and range, with
     * the given dataset constructor.
     *
     * @param i the input dataset.
     * @param c the input record type.
     * @param r the range on which the dataset is split.
     * @param ctor the dataset constructor of the new datasets.
     */
    public SplitTask(Dataset<T> i, Class<T> c, Range<Long> r,
                     DatasetConstructor ctor) {
        super(ctor);
        input = i;
        type = c;
        range = r;
    }

    /**
     * Invokes the task's computation.
     *
     * @return a single invocation over the task's input to a small-enough range
     * of the dataset.
     */
    @Override
    public Dataset<T> compute() {
        if (range.smallEnough()) {
            Dataset<T> localOutput =
                    getDatasetConstructor().constructDataset(type);
            for (long idx = range.begin(); idx < range.end(); idx++) {
                localOutput.add(input.get(idx));
            }
            return localOutput;
        }
        else {
            Pair<Range<Long>, Range<Long>> ranges = range.split();
            SplitTask<T> left = new SplitTask<>(input, type,
                    ranges.first, getDatasetConstructor());
            SplitTask<T> right = new SplitTask<>(input, type,
                    ranges.second, getDatasetConstructor());
            left.fork();
            right.fork();
            Dataset<T> localOutput = left.join();
            localOutput.append(right.join());
            return localOutput;
        }
    }
}
