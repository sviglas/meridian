package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by sviglas on 10/08/15.
 */
public class MapTask<TIn, TOut> extends RecursiveTask<LList<TOut>> {
    private List<TIn> input;
    private Range<Integer> range;
    private MapFunction<TIn, TOut> mapper;

    public MapTask(List<TIn> in, Range<Integer> r,
                   MapFunction<TIn, TOut> f) {
        input = in;
        range = r;
        mapper = f;
    }

    @Override
    protected LList<TOut> compute() {
        if (range.smallEnough()) {
            LList<TOut> localOut = new LList<>();
            for (TIn rin : input.subList(range.begin(), range.end())) {
                TOut rout = mapper.apply(rin);
                localOut.append(rout);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            MapTask<TIn, TOut> left =
                    new MapTask<>(input, ranges.first, mapper);
            MapTask<TIn, TOut> right =
                    new MapTask<>(input, ranges.second, mapper);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }
}
