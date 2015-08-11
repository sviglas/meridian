package net.sviglas.meridian;

import net.sviglas.util.Pair;

import java.util.*;
import java.util.concurrent.RecursiveTask;

/**
 * Created by sviglas on 10/08/15.
 */
public class JoinTask<Tl, Tr, TOut> extends RecursiveTask<LList<TOut>> {
    private List<List<Tl>> leftInput;
    private List<List<Tr>> rightInput;
    private Range<Integer> range;
    private JoinFunction<Tl, Tr, TOut> joiner;

    public JoinTask(List<List<Tl>> l, List<List<Tr>> r,
                    Range<Integer> rg,
                    JoinFunction<Tl, Tr, TOut> j) {
        leftInput = l;
        rightInput = r;
        range = rg;
        joiner = j;
    }

    @Override
    public LList<TOut> compute() {
        if (range.smallEnough()) {
            LList<TOut> localOut = new LList<>();
            for (int i = range.begin(); i < range.end(); i++) {
                List<Tl> localLeft = leftInput.get(i);
                List<Tr> localRight = rightInput.get(i);
                localJoin(localLeft, localRight, localOut);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            JoinTask<Tl, Tr, TOut> left =
                    new JoinTask<>(leftInput, rightInput,
                            ranges.first, joiner);
            JoinTask<Tl, Tr, TOut> right =
                    new JoinTask<>(leftInput, rightInput,
                            ranges.second, joiner);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }

    protected void localJoin(List<Tl> l, List<Tr> r, LList<TOut> out) {
        Map<Integer, List<Tl>> leftMap = new HashMap<>();
        for (Tl lr : l) {
            int hash = lr.hashCode();
            List<Tl> leftBucket = leftMap.get(hash);
            if (leftBucket == null) leftBucket = new ArrayList<>();
            leftBucket.add(lr);
            leftMap.put(lr.hashCode(), leftBucket);
        }
        for (Tr rr : r) {
            List<Tl> leftBucket = leftMap.get(rr.hashCode());
            if (leftBucket != null) {
                for (Tl lr : leftBucket) {
                    if (joiner.areEqual(lr, rr)) {
                        out.append(joiner.combine(lr, rr));
                    }
                }
            }
        }
    }

    public static void main(String [] args) {
        List<Integer> left = Arrays.asList(1, 2, 3, 4, 4, 4, 5, 6, 6);
        List<Integer> right = Arrays.asList(2, 4, 4, 5, 5, 5, 6, 6, 6);
        JoinTask<Integer, Integer, Integer> mt =
                new JoinTask<>(null, null, null,
                        new JoinFunction<Integer, Integer, Integer>() {
                            @Override
                            public boolean areEqual(Integer l, Integer r) {
                                return l.compareTo(r) == 0;
                            }

                            @Override
                            public Integer combine(Integer l, Integer r) {
                                return l;
                            }
                        });
        LList<Integer> out = new LList<>();
        mt.localJoin(left, right, out);
        System.out.println("output: " + out);
    }
}
