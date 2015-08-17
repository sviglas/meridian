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
public class FoldTask<TIn, TOut> extends Task<TOut> {
    private Dataset<TIn> input;
    private Range<Long> range;
    private FoldFunction<TIn, TOut> folder;

    public FoldTask(Dataset<TIn> i, Range<Long> r, FoldFunction<TIn, TOut> f) {
        super();
        input = i;
        range = r;
        folder = f;
    }

    @Override
    public TOut compute() {
        if (range.smallEnough()) {
            TOut localOutput = folder.noop();
            long index = range.begin();
            while (index < range.end()) {
                TIn rin = input.get(index);
                localOutput = folder.accumulate(localOutput, rin);
            }
            return localOutput;
        }
        else {
            Pair<Range<Long>, Range<Long>> ranges = range.split();
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
