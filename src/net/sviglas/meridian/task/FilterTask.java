/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

/**
 * Basic encapsulation of a filtering task, filters a dataset according to
 * a filtering predicate.
 *
 * @param <T> the input type.
 */
public class FilterTask<T> extends Task<Dataset<T>> {
    // the input dataset
    private Dataset<T> input;
    // the enumerating range
    private Range<Long> range;
    // the filtering function
    private FilterFunction<T> filter;

    /**
     * Constructs a new filtering task for the given types and with a default
     * dataset constructor.
     *
     * @param i the input dataset.
     * @param r the input range.
     * @param f the filtering function.
     */
    public FilterTask(Dataset<T> i, Range<Long> r, FilterFunction<T> f) {
        this(i, r, f, new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new filtering task for the given types and dataset
     * constructor.
     *
     * @param i the input dataset.
     * @param r the input range.
     * @param f the filtering function.
     * @param ctor the dataset constructor.
     */
    public FilterTask(Dataset<T> i, Range<Long> r,
                      FilterFunction<T> f, DatasetConstructor ctor) {
        super(ctor);
        input = i;
        range = r;
        filter = f;
    }

    /**
     * Invokes the computation over the dataset by testing the filtering
     * predicate over the dataset records.
     *
     * @return the filtered dataset.
     */
    @Override
    protected Dataset<T> compute() {
        if (range.smallEnough()) {
            Dataset<T> localOutput = getDatasetConstructor().constructDataset(
                    filter.getInputType());
            for (long index = range.begin(); index < range.end(); index++) {
                T t = input.get(index);
                if (filter.filter(t)) localOutput.add(t);
            }
            return localOutput;
        }
        else {
            Pair<Range<Long>, Range<Long>> pair = range.split();
            FilterTask<T> left = new FilterTask<>(input, pair.first,
                    filter, getDatasetConstructor());
            FilterTask<T> right = new FilterTask<>(input, pair.second,
                    filter, getDatasetConstructor());
            left.fork();
            right.fork();
            Dataset<T> localOutput = left.join();
            localOutput.append(right.join());
            return localOutput;
        }
    }
}
