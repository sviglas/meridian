package net.sviglas.meridian;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.RecursiveTask;

import net.sviglas.util.Pair;

public class PartitionTask<TIn, KOut, VOut>
    extends RecursiveTask<SortedMap<KOut, LList<VOut>>> {
    
    private List<TIn> input;
    private Range<Integer> range;
    private PartitionFunction<TIn, KOut, VOut> partitioner;
    
    public PartitionTask(List<TIn> i, Range<Integer> r,
                         PartitionFunction<TIn, KOut, VOut> p) {
        input = i;
        range = r;
        partitioner = p;
    }

    @Override
    protected SortedMap<KOut, LList<VOut>> compute() {
        if (range.smallEnough()) {
            SortedMap<KOut, LList<VOut>> localGroup = new TreeMap<>();
            for (TIn t : input.subList(range.begin(), range.end())) {
                Pair<KOut, VOut> kvout = partitioner.apply(t);
                LList<VOut> values = localGroup.get(kvout.first);
                if (values != null)
                    values.append(kvout.second);
                else {
                    values = new LList<VOut>();
                    values.append(kvout.second);
                    localGroup.put(kvout.first, values);
                }                
            }
            return localGroup;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            PartitionTask<TIn, KOut, VOut> left =
                new PartitionTask<>(input, ranges.first, partitioner);
            PartitionTask<TIn, KOut, VOut> right =
                new PartitionTask<>(input, ranges.second, partitioner);
            left.fork();
            right.fork();
            return merge(left.join(), right.join());
        }
    }
    
    protected SortedMap<KOut, LList<VOut>>
            merge(SortedMap<KOut, LList<VOut>> left,
                  SortedMap<KOut, LList<VOut>> right) {
        SortedMap<KOut, LList<VOut>> small = left;
        SortedMap<KOut, LList<VOut>> big = right;
        if (right.size() < left.size()) { small = right; big = left; }
        for (Map.Entry<KOut, LList<VOut>> entry : small.entrySet()) {
            LList<VOut> smallList = entry.getValue();
            LList<VOut> bigList = big.get(entry.getKey());
            if (bigList != null) bigList.append(smallList);
            else big.put(entry.getKey(), smallList);
        }
        return big;
    }
    
}
