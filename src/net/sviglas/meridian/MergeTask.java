package net.sviglas.meridian;

import net.sviglas.util.Pair;

import java.util.*;
import java.util.concurrent.RecursiveTask;

/**
 * Created by sviglas on 10/08/15.
 */
public class MergeTask<T extends Comparable<T>>
        extends RecursiveTask<LList<T>> {
    private List<List<T>> leftInput;
    private List<List<T>> rightInput;
    private Range<Integer> range;

    public MergeTask(List<List<T>> l, List<List<T>> r,
                     Range<Integer> rg) {
        leftInput = l;
        rightInput = r;
        range = rg;
    }

    @Override
    public LList<T> compute() {
        if (range.smallEnough()) {
            LList<T> localOut = new LList<>();
            for (int i = range.begin(); i < range.end(); i++) {
                List<T> localLeft = leftInput.get(i);
                List<T> localRight = rightInput.get(i);
                localMerge(localLeft, localRight, localOut);
            }
            return localOut;
        }
        else {
            Pair<Range<Integer>, Range<Integer>> ranges = range.split();
            MergeTask<T> left =
                    new MergeTask<>(leftInput, rightInput, ranges.first);
            MergeTask<T> right =
                    new MergeTask<>(leftInput, rightInput, ranges.second);
            left.fork();
            right.fork();
            return left.join().append(right.join());
        }
    }

    protected void localMerge(List<T> l, List<T> r, LList<T> out) {
        Iterator<T> lit = l.iterator();
        Iterator<T> rit = r.iterator();
        if (! lit.hasNext()) {
            while (rit.hasNext()) out.append(rit.next());
            return;
        }
        if (! rit.hasNext()) {
            while (lit.hasNext()) out.append(lit.next());
            return;
        }
        T litem = lit.next();
        T ritem = rit.next();
        while (lit.hasNext() && rit.hasNext()) {
            while (litem.compareTo(ritem) <= 0) {
                out.append(litem);
                if (lit.hasNext()) litem = lit.next();
                else {
                    out.append(ritem);
                    while (rit.hasNext()) out.append(rit.next());
                    return;
                }
            }
            while (ritem.compareTo(litem) < 0) {
                out.append(ritem);
                if (rit.hasNext()) ritem = rit.next();
                else {
                    out.append(litem);
                    while (lit.hasNext()) out.append(lit.next());
                    return;
                }
            }
        }
    }

    public static void main(String [] args) {
        List<Integer> left = Arrays.asList(1, 2, 3, 4, 4, 4, 5, 6, 6);
        List<Integer> right = Arrays.asList(2, 4, 4, 5, 5, 5, 6, 6, 6);
        MergeTask<Integer> mt = new MergeTask<>(null, null, null);
        LList<Integer> out = new LList<>();
        mt.localMerge(left, right, out);
        System.out.println("output: " + out);
    }
}
