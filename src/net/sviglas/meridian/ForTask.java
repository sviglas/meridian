package net.sviglas.meridian;

import java.util.List;
import java.util.concurrent.RecursiveTask;

import net.sviglas.util.Pair;

public class ForTask <RIn, ROut> extends RecursiveTask<LList<ROut>> {
    private List<RIn> input;
    private Range<Integer> range;
    private ForFunction<RIn, ROut> forFunction;
    
    public ForTask(List<RIn> i, Range<Integer> r, ForFunction<RIn, ROut> f) {
        input = i;
        range = r;
        forFunction = f;
    }

    @Override
    protected LList<ROut> compute() {
        if (range.smallEnough()) {
            LList<ROut> localList = new LList<ROut>();
            for (RIn r : input.subList(range.begin(), range.end()))
                localList.append(forFunction.apply(r));
            return localList;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            ForTask<RIn, ROut> left =
                new ForTask<RIn, ROut>(input, ranges.first, forFunction);
            ForTask<RIn, ROut> right =
                new ForTask<RIn, ROut>(input, ranges.second, forFunction);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }    
}
