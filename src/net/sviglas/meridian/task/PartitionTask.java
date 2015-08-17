package net.sviglas.meridian.task;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.RecursiveTask;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

public class PartitionTask<TIn, KOut extends Comparable<? super KOut>, VOut>
        extends Task<SortedMap<KOut, Dataset<VOut>>> {
    private Dataset<TIn> input;
    private Range<Long> range;
    private PartitionFunction<TIn, KOut, VOut> partitioner;
    
    public PartitionTask(Dataset<TIn> i, Range<Long> r,
                         PartitionFunction<TIn, KOut, VOut> p) {
        this(i, r, p, new DefaultDatasetConstructor());
    }

    public PartitionTask(Dataset<TIn> i, Range<Long> r,
                         PartitionFunction<TIn, KOut, VOut> p,
                         DatasetConstructor ctor) {
        super(ctor);
        input = i;
        range = r;
        partitioner = p;
    }

    @Override
    protected SortedMap<KOut, Dataset<VOut>> compute() {
        if (range.smallEnough()) {
            SortedMap<KOut, Dataset<VOut>> localGroup = new TreeMap<>();
            for (long index = range.begin(); index < range.end(); index++) {
                TIn t = input.get(index);
                Pair<KOut, VOut> kvout = partitioner.partition(t);
                Dataset<VOut> values = localGroup.get(kvout.first);
                if (values != null)
                    values.add(kvout.second);
                else {
                    values = getDatasetConstructor().constructDataset(
                            partitioner.getOutputValueType());
                    values.add(kvout.second);
                    localGroup.put(kvout.first, values);
                }
            }
            return localGroup;
        }
        else {
            Pair<Range<Long>, Range<Long>> ranges = range.split();
            PartitionTask<TIn, KOut, VOut> left =
                new PartitionTask<>(input, ranges.first,
                        partitioner, getDatasetConstructor());
            PartitionTask<TIn, KOut, VOut> right =
                new PartitionTask<>(input, ranges.second,
                        partitioner, getDatasetConstructor());
            left.fork();
            right.fork();
            return merge(left.join(), right.join());
        }
    }
    
    protected SortedMap<KOut, Dataset<VOut>>
            merge(SortedMap<KOut, Dataset<VOut>> left,
                  SortedMap<KOut, Dataset<VOut>> right) {
        SortedMap<KOut, Dataset<VOut>> small = left;
        SortedMap<KOut, Dataset<VOut>> big = right;
        if (right.size() < left.size()) { small = right; big = left; }
        for (Map.Entry<KOut, Dataset<VOut>> entry : small.entrySet()) {
            Dataset<VOut> smallList = entry.getValue();
            Dataset<VOut> bigList = big.get(entry.getKey());
            if (bigList != null) bigList.append(smallList);
            else big.put(entry.getKey(), smallList);
        }
        return big;
    }
    
}
