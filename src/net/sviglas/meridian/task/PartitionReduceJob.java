package net.sviglas.meridian.task;

import java.util.List;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.SortedSet;

import java.util.concurrent.ForkJoinPool;

import net.sviglas.util.Pair;

public class PartitionReduceJob<TIn,
        KIntermediate extends Comparable<? super KIntermediate>, VIntermediate,
        VOut> {
    private List<TIn> input;
    //private Extractor<TIn, KInput, VInput> extractor;
    private PartitionFunction<TIn, KIntermediate, VIntermediate> partitioner;
    private ReduceFunction<KIntermediate, VIntermediate, VOut> reducer;

    public PartitionReduceJob(List<TIn> i,
                              PartitionFunction<TIn,
                                      KIntermediate, VIntermediate> p,
                              ReduceFunction<KIntermediate, VIntermediate,
                                      VOut> r) {
        input = i;
        partitioner = p;
        reducer = r;
    }
                        

    public LList<Pair<KIntermediate, VOut>> execute(ForkJoinPool pool) {
        PartitionTask<TIn, KIntermediate, VIntermediate> partitionTask =
            new PartitionTask<>(input, new IndexRange(0, input.size()),
                          partitioner);
        SortedMap<KIntermediate, LList<VIntermediate>> partitions =
            pool.invoke(partitionTask);
        KeyRange<KIntermediate> kr =
                new KeyRange<>((SortedSet<KIntermediate>) partitions.keySet());
        ReduceTask<KIntermediate, VIntermediate, VOut> reduceTask =
            new ReduceTask<>(partitions, kr, reducer);
        return pool.invoke(reduceTask);
    }

    public static void main(String [] args) {
        try {
            ForkJoinPool forkJoinPool = new ForkJoinPool();
            
            int limit = 100;
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < limit; i++) list.add((i % 10)*(i % 10));

            Identity<Integer> identity = new Identity<>();
            PartitionReduceJob<Integer, Integer, Integer, String>
                job = new PartitionReduceJob<>(list, identity, identity);
            LList<Pair<Integer, String>> rout = job.execute(forkJoinPool);
            System.out.println(rout);
            System.out.println(rout.size());
        }
        catch (Exception e) {
            System.err.println("exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
