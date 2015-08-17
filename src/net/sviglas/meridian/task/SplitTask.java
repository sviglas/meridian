package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */
public class SplitTask<T> extends RecursiveTask<Dataset<T>> {
    private Dataset<T> input;
    private Class<T> type;
    private Range<Long> range;
    private DatasetConstructor datasetConstructor;

    public SplitTask(Dataset<T> i, Class<T> c, Range<Long> r) {
        this(i, c, r, new DefaultDatasetConstructor());
    }

    public SplitTask(Dataset<T> i, Class<T> c, Range<Long> r,
                     DatasetConstructor ctor) {
        input = i;
        type = c;
        range = r;
        datasetConstructor = ctor;
    }

    @Override
    public Dataset<T> compute() {
        if (range.smallEnough()) {
            Dataset<T> localOutput = datasetConstructor.constructDataset(type);
            for (long idx = range.begin(); idx < range.end(); idx++) {
                localOutput.add(input.get(idx));
            }
            return localOutput;
        }
        else {
            Pair<Range<Long>, Range<Long>> ranges = range.split();
            SplitTask<T> left = new SplitTask<>(input, type,
                    ranges.first, datasetConstructor);
            SplitTask<T> right = new SplitTask<>(input, type,
                    ranges.second, datasetConstructor);
            left.fork();
            right.fork();
            Dataset<T> localOutput = left.join();
            localOutput.append(right.join());
            return localOutput;
        }
    }
}
