/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */

package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstraction of a join task between lists of datasets.
 *
 * @param <Tl> the type of the left input dataset records.
 * @param <Tr> the type of the right input dataset records.
 * @param <TOut> the type of the output dataset records.
 */
public class JoinTask<Tl, Tr, TOut> extends Task<Dataset<TOut>> {
    // the lest input
    private List<Dataset<Tl>> leftInput;
    // the right input
    private List<Dataset<Tr>> rightInput;
    // the enumerating range
    private Range<Integer> range;
    // the join function
    private JoinFunction<Tl, Tr, TOut> joiner;

    /**
     * Constructs a new join task given its inputs and with a default dataset
     * constructor.
     *
     * @param l the left input.
     * @param r the right input.
     * @param rg the enumerating range.
     * @param j the join function.
     */
    public JoinTask(List<Dataset<Tl>> l, List<Dataset<Tr>> r,
                    Range<Integer> rg,
                    JoinFunction<Tl, Tr, TOut> j) {
        this(l, r, rg, j, new DefaultDatasetConstructor());
    }

    /**
     * Constructs a new join task given its inputs and dataset constructor.
     *
     * @param l the left input.
     * @param r the right input.
     * @param rg the enumerating range.
     * @param j the join function.
     * @param ctor the dataset constructor.
     */
    public JoinTask(List<Dataset<Tl>> l, List<Dataset<Tr>> r,
                    Range<Integer> rg,
                    JoinFunction<Tl, Tr, TOut> j,
                    DatasetConstructor ctor) {
        super(ctor);
        leftInput = l;
        rightInput = r;
        range = rg;
        joiner = j;
    }

    /**
     * Invokes the join computation: for corresponding datasets from the input
     * lists, it joins them by finding equal elements.
     *
     * @return the output of the join computation.
     */
    @Override
    public Dataset<TOut> compute() {
        if (range.smallEnough()) {
            Dataset<TOut> localOut = getDatasetConstructor().constructDataset(
                    joiner.getOutputValueType());
            for (int i = range.begin(); i < range.end(); i++) {
                Dataset<Tl> localLeft = leftInput.get(i);
                Dataset<Tr> localRight = rightInput.get(i);
                localJoin(localLeft, localRight, localOut);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            JoinTask<Tl, Tr, TOut> left =
                    new JoinTask<>(leftInput, rightInput,
                            ranges.first, joiner, getDatasetConstructor());
            JoinTask<Tl, Tr, TOut> right =
                    new JoinTask<>(leftInput, rightInput,
                            ranges.second, joiner, getDatasetConstructor());
            left.fork();
            right.fork();
            Dataset<TOut> localOut = left.join();
            localOut.append(right.join());
            return localOut;
        }
    }

    /**
     * Internal method to compute the local join between dataset.
     *
     * @param l the left input.
     * @param r the right input.
     * @param out the output dataset.
     */
    protected void localJoin(Dataset<Tl> l, Dataset<Tr> r, Dataset<TOut> out) {
        Map<Integer, Dataset<Tl>> leftMap = new HashMap<>();
        for (Tl lr : l) {
            int hash = lr.hashCode();
            Dataset<Tl> leftBucket = leftMap.get(hash);
            if (leftBucket == null) {
                leftBucket = getDatasetConstructor().constructDataset(
                        joiner.getLeftInputType());
            }
            leftBucket.add(lr);
            leftMap.put(lr.hashCode(), leftBucket);
        }
        for (Tr rr : r) {
            Dataset<Tl> leftBucket = leftMap.get(rr.hashCode());
            if (leftBucket != null) {
                for (Tl lr : leftBucket) {
                    if (joiner.equal(lr, rr)) {
                        out.add(joiner.combine(lr, rr));
                    }
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
        Dataset<Integer> left =
                new net.sviglas.meridian.storage.ArrayStore<>(Integer.class);
        for (int i = 0; i < 10; i++) for (int j = 0; j < i; j++) left.add(j);
        Dataset<Integer> right =
                new net.sviglas.meridian.storage.ArrayStore<>(Integer.class);
        for (int i = 0; i < 10; i++) for (int j = 0; j < i; j++) left.add(j);
        JoinTask<Integer, Integer, Integer> mt =
                new JoinTask<>(null, null, null,
                        new JoinFunction<Integer, Integer, Integer>(
                                Integer.class, Integer.class, Integer.class) {
                            @Override
                            public boolean equal(Integer l, Integer r) {
                                return l.compareTo(r) == 0;
                            }

                            @Override
                            public Integer combine(Integer l, Integer r) {
                                return l;
                            }
                        }, null);
        Dataset<Integer> out =
                new net.sviglas.meridian.storage.ArrayStore<>(Integer.class);
        mt.localJoin(left, right, out);
        out.print(System.out);
    }
}
