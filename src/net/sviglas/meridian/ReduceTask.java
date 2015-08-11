package net.sviglas.meridian;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.RecursiveTask;

import net.sviglas.util.Pair;

public class ReduceTask<KIn, VIn, VOut>
        extends RecursiveTask<LList<Pair<KIn, VOut>>> {

    private SortedMap<KIn, LList<VIn>> input;
    private Range<KIn> range;
    private ReduceFunction<KIn, VIn, VOut> reducer;

    public ReduceTask(SortedMap<KIn, LList<VIn>> i,
                      Range<KIn> r,
                      ReduceFunction<KIn, VIn, VOut> rd) {
        input = i;
        range = r;
        reducer = rd;
    }

    @Override
    protected LList<Pair<KIn, VOut>> compute() {
        if (range.smallEnough()) {
            LList<Pair<KIn, VOut>> localList = new LList<Pair<KIn, VOut>>();
            Iterator<KIn> iterator = range.iterator();
            while (iterator.hasNext()) {
                KIn key = iterator.next();
                LList<VIn> value = input.get(key);
                Pair<KIn, VOut> pair =
                    new Pair<KIn, VOut>(key,
                                        reducer.apply(key, value.iterator()));
                localList.append(pair);
            }
            return localList;
        }
        else {
            Pair<Range<KIn>, Range<KIn>> ranges = range.split();
            ReduceTask<KIn, VIn, VOut> left =
                new ReduceTask<KIn, VIn, VOut>(input, ranges.first, reducer);
            ReduceTask<KIn, VIn, VOut> right =
                new ReduceTask<KIn, VIn, VOut>(input, ranges.second, reducer);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }
}
