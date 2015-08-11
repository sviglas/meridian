package net.sviglas.meridian.task;

import java.util.List;
import java.util.concurrent.RecursiveTask;

import net.sviglas.util.Pair;

//public class FilterTask <R> extends RecursiveTask<LList<R>> {
public class FilterTask<T> extends RecursiveTask<LList<T>> {
    private List<T> input;
    private Range<Integer> range;
    private FilterFunction<T> filter;
    
    public FilterTask(List<T> i, Range<Integer> r, FilterFunction<T> f) {
        input = i;
        range = r;
        filter = f;
    }

    @Override
    protected LList<T> compute() {
        if (range.smallEnough()) {
            LList<T> localList = new LList<>();
            for (T t : input.subList(range.begin(), range.end()))
                if (filter.apply(t)) localList.append(t);
            return localList;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> pair = range.split();
            FilterTask<T> left = new FilterTask<>(input, pair.first, filter);
            FilterTask<T> right = new FilterTask<>(input, pair.second, filter);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }
}
