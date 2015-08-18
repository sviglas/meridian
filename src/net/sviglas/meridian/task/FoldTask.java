/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Basic abstraction of a folding task; folds an input dataset into a single
 * output value.
 *
 * @param <TIn> the input type.
 * @param <TOut> the output type.
 */
public class FoldTask<TIn, TOut> extends Task<TOut> {
    // the input dataset
    private Dataset<TIn> input;
    // the enumerating range
    private Range<Long> range;
    // the folding function
    private FoldFunction<TIn, TOut> folder;

    /**
     * Constructs a new task given input parameters and folding function.
     *
     * @param i the input dataset.
     * @param r the enumerating range.
     * @param f the folding function.
     */
    public FoldTask(Dataset<TIn> i, Range<Long> r, FoldFunction<TIn, TOut> f) {
        super();
        input = i;
        range = r;
        folder = f;
    }

    /**
     * Invokes the folding computation by continuously folding the records of
     * the input dataset into a fixed-point.
     *
     * @return the output of the fold.
     */
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
