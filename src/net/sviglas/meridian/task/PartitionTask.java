/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

/**
 * Basic abstraction of a partitioning task.  Splits a dataset into multiple
 * keyed partitions.
 *
 * @param <TIn> the input type.
 * @param <KOut> the output key type.
 * @param <VOut> the output value type.
 */
public class PartitionTask<TIn, KOut extends Comparable<? super KOut>, VOut>
        extends Task<SortedMap<KOut, Dataset<VOut>>> {
    // the input dataset
    private Dataset<TIn> input;
    // the range over the input
    private Range<Long> range;
    // the partitioner function
    private PartitionFunction<TIn, KOut, VOut> partitioner;

    /**
     * Constructs a new partitioning task for the given parameters and with the
     * default dataset constructor.
     *
     * @param i the input dataset.
     * @param r the input range splitter.
     * @param p the partitioning function.
     */
    public PartitionTask(Dataset<TIn> i, Range<Long> r,
                         PartitionFunction<TIn, KOut, VOut> p) {
        this(i, r, p, new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new partitioning task for the given parameters and
     * dataset constructor.
     *
     * @param i the input dataset.
     * @param r the input range splitter.
     * @param p the partitioning function.
     * @param ctor the dataset constructor.
     */
    public PartitionTask(Dataset<TIn> i, Range<Long> r,
                         PartitionFunction<TIn, KOut, VOut> p,
                         DatasetConstructor ctor) {
        super(ctor);
        input = i;
        range = r;
        partitioner = p;
    }

    /**
     * Invokes the partitioning function by iterating over all elements of the
     * input dataset and partitioning them into a keyed map of datasets.
     *
     * @return the output map.
     */
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

    /**
     * Merges two maps on their keys.
     *
     * @param left the left input map.
     * @param right the right input map.
     * @return the output map.
     */
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
