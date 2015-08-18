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
 * Basic abstraction for a functional map task.  Given an input dataset, maps a
 * function over its records to produce a new dataset of the same size.
 *
 * @param <TIn> the input type.
 * @param <TOut> the output type.
 */
public class MapTask<TIn, TOut> extends Task<Dataset<TOut>> {
    // the input dataset
    private Dataset<TIn> input;
    // the enumerating range
    private Range<Long> range;
    // the mapping function
    private MapFunction<TIn, TOut> mapper;

    /**
     * Constructs a new mapping task for the given parameters and with the
     * default dataset constructor.
     *
     * @param in the input dataset.
     * @param r the input range splitter.
     * @param f the mapping function.
     */
    public MapTask(Dataset<TIn> in, Range<Long> r,
                   MapFunction<TIn, TOut> f) {
        this(in, r, f, new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new mapping task for the given parameters and dataset
     * constructor.
     *
     * @param in the input dataset.
     * @param r the input range splitter.
     * @param f the mapping function.
     * @param ctor the dataset constructor.
     */
    public MapTask(Dataset<TIn> in, Range<Long> r,
                   MapFunction<TIn, TOut> f,
                   DatasetConstructor ctor) {
        super(ctor);
        input = in;
        range = r;
        mapper = f;
    }

    /**
     * Invokes the mapping computation: for each element of the input,
     * it applies the mapping function generating a new dataset.
     *
     * @return the resulting dataset.
     */
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
