package net.sviglas.meridian.task;

import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

import java.util.*;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */
public class JoinTask<Tl, Tr, TOut> extends Task<Dataset<TOut>> {
    private List<Dataset<Tl>> leftInput;
    private List<Dataset<Tr>> rightInput;
    private Range<Integer> range;
    private JoinFunction<Tl, Tr, TOut> joiner;

    public JoinTask(List<Dataset<Tl>> l, List<Dataset<Tr>> r,
                    Range<Integer> rg,
                    JoinFunction<Tl, Tr, TOut> j) {
        this(l, r, rg, j, new DefaultDatasetConstructor());
    }

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
        System.out.println("output: " + out);
    }
}
