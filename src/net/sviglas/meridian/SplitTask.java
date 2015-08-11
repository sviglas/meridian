package net.sviglas.meridian;

import net.sviglas.util.Pair;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by sviglas on 11/08/15.
 */
public class SplitTask<T> extends RecursiveTask<LList<T>> {
    public List<T> input;
    public Range<Integer> range;

    public SplitTask(List<T> i, Range<Integer> r) {
        input = i;
        range = r;
    }

    @Override
    public LList<T> compute() {
        if (range.smallEnough()) {
            LList<T> localOut = new LList<>();
            for (T t : input.subList(range.begin(), range.end())) {
                localOut.append(t);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            SplitTask<T> left = new SplitTask<>(input, ranges.first);
            SplitTask<T> right = new SplitTask<>(input, ranges.second);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }
}
