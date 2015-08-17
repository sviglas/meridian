package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */
public class MapTask<TIn, TOut> extends Task<Dataset<TOut>> {
    private Dataset<TIn> input;
    private Range<Long> range;
    private MapFunction<TIn, TOut> mapper;

    public MapTask(Dataset<TIn> in, Range<Long> r,
                   MapFunction<TIn, TOut> f) {
        this(in, r, f, new DefaultDatasetConstructor());
    }

    public MapTask(Dataset<TIn> in, Range<Long> r,
                   MapFunction<TIn, TOut> f,
                   DatasetConstructor ctor) {
        super(ctor);
        input = in;
        range = r;
        mapper = f;
    }

    @Override
    protected Dataset<TOut> compute() {
        if (range.smallEnough()) {
            Dataset<TOut> localOutput =
                    getDatasetConstructor().constructDataset(
                            mapper.getOutputType());
            for (long index = range.begin(); index < range.end(); index++) {
                localOutput.add(mapper.map(input.get(index)));
            }
            return localOutput;
        }
        else {
            Pair<Range<Long>, Range<Long>> ranges = range.split();
            MapTask<TIn, TOut> left = new MapTask<>(input, ranges.first,
                    mapper, getDatasetConstructor());
            MapTask<TIn, TOut> right = new MapTask<>(input, ranges.second,
                    mapper, getDatasetConstructor());
            left.fork();
            right.fork();
            Dataset<TOut> localOutput = left.join();
            localOutput.append(right.join());
            return localOutput;
        }
    }
}
