/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.RecursiveTask;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

/**
 * Constructs a reduce task that transforms a map of key-value-list pairs to a
 * map of key-reduced-value pairs.
 *
 * @param <KIn> the incoming key type.
 * @param <VIn> the incoming value type.
 * @param <VOut> the resulting value type.
 */

public class ReduceTask<KIn, VIn, VOut> extends Task<SortedMap<KIn, VOut>> {
    // the input dataset
    private SortedMap<KIn, Dataset<VIn>> input;
    // the incoming range
    private Range<KIn> range;
    // the reduce function
    private ReduceFunction<KIn, VIn, VOut> reducer;

    /**
     * Constructs a new reduce task given its input map, range, and reducer.
     *
     * @param i the input map.
     * @param r the incoming range.
     * @param rd the reduce function.
     */
    public ReduceTask(SortedMap<KIn, Dataset<VIn>> i,
                      Range<KIn> r,
                      ReduceFunction<KIn, VIn, VOut> rd) {
        super();
        input = i;
        range = r;
        reducer = rd;
    }

    /**
     * Invokes the reduce function by iterating over all elements of the
     * input map and running the reduce function over the list of values.
     *
     * @return the output map.
     */
    @Override
    protected SortedMap<KIn, VOut> compute() {
        if (range.smallEnough()) {
            SortedMap<KIn, VOut> localOutput = new TreeMap<>();
            Iterator<KIn> iterator = range.iterator();
            while (iterator.hasNext()) {
                KIn key = iterator.next();
                Dataset<VIn> value = input.get(key);
                localOutput.put(key, reducer.reduce(key, value.iterator()));
            }
            return localOutput;
        }
        else {
            Pair<Range<KIn>, Range<KIn>> ranges = range.split();
            ReduceTask<KIn, VIn, VOut> left =
                new ReduceTask<>(input, ranges.first, reducer);
            ReduceTask<KIn, VIn, VOut> right =
                new ReduceTask<>(input, ranges.second, reducer);
            left.fork();
            right.fork();
            SortedMap<KIn, VOut> localOutput = left.join();
            localOutput.putAll(right.join());
            return localOutput;
        }
    }
}
