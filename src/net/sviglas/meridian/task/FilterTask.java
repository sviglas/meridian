package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

public class FilterTask<T> extends Task<Dataset<T>> {
    private Dataset<T> input;
    private Range<Long> range;
    private FilterFunction<T> filter;

    public FilterTask(Dataset<T> i, Range<Long> r, FilterFunction<T> f) {
        this(i, r, f, new DefaultDatasetConstructor());
    }
    
    public FilterTask(Dataset<T> i, Range<Long> r,
                      FilterFunction<T> f, DatasetConstructor ctor) {
        super(ctor);
        input = i;
        range = r;
        filter = f;
    }

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
