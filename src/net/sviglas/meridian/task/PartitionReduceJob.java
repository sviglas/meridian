package net.sviglas.meridian.task;

import java.util.*;

import java.util.concurrent.ForkJoinPool;

import net.sviglas.meridian.storage.ArrayStore;
import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

public class PartitionReduceJob<TIn,
        KIntermediate extends Comparable<? super KIntermediate>, VIntermediate,
        VOut> {
    private Dataset<TIn> input;
    //private Extractor<TIn, KInput, VInput> extractor;
    private PartitionFunction<TIn, KIntermediate, VIntermediate> partitioner;
    private ReduceFunction<KIntermediate, VIntermediate, VOut> reducer;

    public PartitionReduceJob(Dataset<TIn> i,
                              PartitionFunction<TIn,
                                      KIntermediate, VIntermediate> p,
                              ReduceFunction<KIntermediate, VIntermediate,
                                      VOut> r) {
        input = i;
        partitioner = p;
        reducer = r;
    }
                        

    public SortedMap<KIntermediate, VOut> execute(ForkJoinPool pool) {
        PartitionTask<TIn, KIntermediate, VIntermediate> partitionTask =
            new PartitionTask<>(input, new IndexRange(0, input.size()),
                          partitioner);
        SortedMap<KIntermediate, Dataset<VIntermediate>> partitions =
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
            Dataset<Integer> list = new ArrayStore<>(Integer.class);
            for (int i = 0; i < limit; i++) list.add((i % 10)*(i % 10));

            PartitionReduceJob<Integer, Integer, Integer, String>
                job = new PartitionReduceJob<>(list,
                    new PartitionFunction<Integer, Integer, Integer>(
                            Integer.class, Integer.class, Integer.class) {
                        @Override
                        public Pair<Integer, Integer> partition(Integer t) {
                            return new Pair<>(t, t);
                        }
                    },
                    new ReduceFunction<Integer, Integer, String>(
                            Integer.class, Integer.class, String.class) {
                        @Override
                        public String reduce(Integer k, Iterator<Integer> v) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("[").append(k).append("->");
                            for (; v.hasNext();) sb.append(v.next()).append("_");
                            sb.setLength(sb.length()-1);
                            sb.append("]");
                            return sb.toString();
                        }
                    }
            );
            SortedMap<Integer, String> rout = job.execute(forkJoinPool);
            System.out.println(rout);
            System.out.println(rout.size());
        }
        catch (Exception e) {
            System.err.println("exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
