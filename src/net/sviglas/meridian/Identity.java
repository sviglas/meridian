package net.sviglas.meridian;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ForkJoinPool;

import net.sviglas.util.Pair;

public class Identity <K, V> implements Extractor<Pair<K, V>, K, V>,
        PartitionFunction<K, K, V>, ReduceFunction <K, V, String> {

    public Pair<K, V> extract(Pair<K, V> in) { return in; }
    public Pair<K, V> apply(K in) { return null; }
    public String apply(K k, Iterator<V> i) {
        StringBuffer sb = new StringBuffer();
        sb.append("[" + k + "->");
        for (; i.hasNext();) sb.append(i.next() + "_");
        sb.setLength(sb.length()-1);
        sb.append("]");
        return sb.toString();
    }
/*
    public static void main(String [] args) {
        try {
            ForkJoinPool forkJoinPool = new ForkJoinPool();

            int limit = 100;
            List<Pair<Integer, String>> list =
                new ArrayList<Pair<Integer, String>>();
            for (int i = 0; i < limit; i++) {
                Pair<Integer, String> pair =
                    new Pair<Integer, String>(i % 10, "j" + (i*i));
                list.add(pair);
            }

            Identity<Integer, String> identity = new Identity<Integer, String>();
            Range<Integer> range = new IndexRange(0, list.size(), 10);
            PartitionTask<Pair<Integer, String>, Integer, String, Integer, String> task =
                new PartitionTask<Pair<Integer, String>, Integer, String, Integer, String>(list, range, identity, identity);
            SortedMap<Integer, LList<String>> out = forkJoinPool.invoke(task);
            System.out.println(out);
            System.out.println(out.size());

            ReduceTask<Integer, String, String> rtask =
                new ReduceTask<Integer, String, String>(out, new KeyRange<Integer>((SortedSet<Integer>)out.keySet()), identity);
            LList<Pair<Integer, String>> rout = forkJoinPool.invoke(rtask);
            System.out.println(rout);
            System.out.println(rout.size());
        }
        catch (Exception e) {
            System.err.println("exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
    */
}
