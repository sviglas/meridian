package net.sviglas.meridian.task;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.RecursiveTask;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

public class ReduceTask<KIn, VIn, VOut>
        extends Task<SortedMap<KIn, VOut>> {

    private SortedMap<KIn, Dataset<VIn>> input;
    private Range<KIn> range;
    private ReduceFunction<KIn, VIn, VOut> reducer;

    public ReduceTask(SortedMap<KIn, Dataset<VIn>> i,
                      Range<KIn> r,
                      ReduceFunction<KIn, VIn, VOut> rd) {
        super();
        input = i;
        range = r;
        reducer = rd;
    }

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
