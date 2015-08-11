package net.sviglas.meridian;

import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by sviglas on 11/08/15.
 */
public class FoldTask<TIn, TOut> extends RecursiveTask<TOut> {
    private List<TIn> input;
    private Range<Integer> range;
    private FoldFunction<TIn, TOut> folder;

    public FoldTask(List<TIn> i, Range<Integer> r, FoldFunction<TIn, TOut> f) {
        input = i;
        range = r;
        folder = f;
    }

    @Override
    public TOut compute() {
        if (range.smallEnough()) {
            TOut localOut = folder.noop();
            for (TIn rin : input.subList(range.begin(), range.end())) {
                localOut = folder.apply(localOut, rin);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            FoldTask<TIn, TOut> left =
                    new FoldTask<>(input, ranges.first, folder);
            FoldTask<TIn, TOut> right =
                    new FoldTask<>(input, ranges.second, folder);
            left.fork();
            right.fork();
            return folder.combine(left.join(), right.join());
        }
    }
}
