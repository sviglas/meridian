/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.ArrayStore;
import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.*;

/**
 * Merges two split input datasets depending on the order of their elements.
 *
 * @param <T> the input type.
 */
public class MergeTask<T> extends Task<Dataset<T>> {
    // the left input
    private List<Dataset<T>> leftInput;
    // the right input
    private List<Dataset<T>> rightInput;
    // the input range
    private Range<Integer> range;
    // the merging function
    private MergeFunction<T> merger;

    /**
     * Construct a new merge task given inputs and with the default dataset
     * constructor.
     *
     * @param l the left input.
     * @param r the right input.
     * @param rg the range enumerator.
     * @param m the merging function.
     */
    public MergeTask(List<Dataset<T>> l, List<Dataset<T>> r,
                     Range<Integer> rg, MergeFunction<T> m) {
        this(l, r, rg, m, new DefaultDatasetConstructor());
    }

    /**
     * Construct a new merge task given inputs and dataset constructor.
     *
     * @param l the left input.
     * @param r the right input.
     * @param rg the range enumerator.
     * @param m the merging function.
     * @param ctor the dataset constructor.
     */
    public MergeTask(List<Dataset<T>> l, List<Dataset<T>> r,
                     Range<Integer> rg, MergeFunction<T> m,
                     DatasetConstructor ctor) {
        super(ctor);
        leftInput = l;
        rightInput = r;
        range = rg;
        merger = m;
    }

    /**
     * Invokes the task computation; merges corresponding datasets by the
     * merging comparator into a large output dataset.
     *
     * @return the merged input datasets.
     */
    @Override
    public Dataset<T> compute() {
        if (range.smallEnough()) {
            Dataset<T> localOutput = getDatasetConstructor().constructDataset(
                    merger.getInputType());
            for (int i = range.begin(); i < range.end(); i++) {
                Dataset<T> localLeft = leftInput.get(i);
                Dataset<T> localRight = rightInput.get(i);
                localMerge(localLeft, localRight, localOutput);
            }
            return localOutput;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            MergeTask<T> left = new MergeTask<>(leftInput, rightInput,
                    ranges.first, merger, getDatasetConstructor());
            MergeTask<T> right = new MergeTask<>(leftInput, rightInput,
                    ranges.second, merger, getDatasetConstructor());
            left.fork();
            right.fork();
            Dataset<T> localOutput = left.join();
            localOutput.append(right.join());
            return localOutput;
        }
    }

    /**
     * Helper method to locally merge two datasets.
     *
     * @param l the left dataset.
     * @param r the right dataset.
     * @param out the output dataset to receive the merged inputs.
     */
    protected void localMerge(Dataset<T> l, Dataset<T> r, Dataset<T> out) {
        Iterator<T> lit = l.iterator();
        Iterator<T> rit = r.iterator();
        if (! lit.hasNext()) {
            while (rit.hasNext()) out.add(rit.next());
            return;
        }
        if (! rit.hasNext()) {
            while (lit.hasNext()) out.add(lit.next());
            return;
        }
        T litem = lit.next();
        T ritem = rit.next();
        while (lit.hasNext() && rit.hasNext()) {
            while (merger.compare(litem, ritem) <= 0) {
                out.add(litem);
                if (lit.hasNext()) litem = lit.next();
                else {
                    out.add(ritem);
                    while (rit.hasNext()) out.add(rit.next());
                    return;
                }
            }
            while (merger.compare(ritem, litem) < 0) {
                out.add(ritem);
                if (rit.hasNext()) ritem = rit.next();
                else {
                    out.add(litem);
                    while (lit.hasNext()) out.add(lit.next());
                    return;
                }
            }
        }
    }

    /**
     * Debug main.
     *
     * @param args parameters.
     */
    public static void main(String [] args) {
        Dataset<Integer> left = new ArrayStore<>(Integer.class);
        left.add(1);    left.add(2);    left.add(3);    left.add(4);
        left.add(4);    left.add(4);    left.add(5);    left.add(6);
        left.add(6);
        Dataset<Integer> right = new ArrayStore<>(Integer.class);
        right.add(2);   right.add(4);   right.add(4);   right.add(5);
        right.add(5);   right.add(5);   right.add(6);   right.add(6);
        right.add(6);
        MergeTask<Integer> mt = new MergeTask<>(null, null, null,
                new MergeFunction<Integer>(Integer.class) {
                    @Override
                    public int compare(Integer l, Integer r) {
                        return l.compareTo(r);
                    }
                });
        Dataset<Integer> out = new ArrayStore<>(Integer.class);
        mt.localMerge(left, right, out);
        out.print(System.out);
    }
}
