package net.sviglas.meridian;

import net.sviglas.util.Pair;

public class NewFilterTask<R> extends Task<R> {
    private Task<R> inputTask;
    private LList<R> input;
    private Range<Integer> range;
    private FilterFunction<R> filter;
    
    public NewFilterTask(Task<R> i, Range<Integer> r, FilterFunction<R> f) {
        inputTask = i;
        input = null;
        range = r;
        filter = f;
    }    

    @Override
    protected LList<R> compute() {
        if (range.smallEnough()) {
            LList<R> localList = new LList<R>();
            /*
            for (R r : input.subList(range.begin(), range.end()))
                if (filter.apply(r)) localList.append(r);
            */
            return localList;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> pair = range.split();
            /*
            FilterTask<R> left = new FilterTask<R>(input, pair.first, filter);
            FilterTask<R> right = new FilterTask<R>(input, pair.second, filter);
            left.fork();
            right.fork();
            return left.join().append(right.join());
            */
            return null;
        }
    }
}
