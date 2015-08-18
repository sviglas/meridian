/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 11/08/15.
 */

package net.sviglas.meridian.task;

import java.util.*;

import java.util.concurrent.ForkJoinPool;

import net.sviglas.meridian.storage.ArrayStore;
import net.sviglas.meridian.storage.Dataset;
import net.sviglas.util.Pair;

/**
 * An example of a partition-reduce job (the equivalent of map-reduce).
 *
 * @param <TIn> the input type.
 * @param <KIntermediate> the type of intermediate keys.
 * @param <VIntermediate> the type of intermediate values.
 * @param <VOut> the output type.
 */

public class PartitionReduceJob<TIn,
        KIntermediate extends Comparable<? super KIntermediate>,
        VIntermediate, VOut> {
    // the input dataset
    private Dataset<TIn> input;
    // the partitioner function
    private PartitionFunction<TIn, KIntermediate, VIntermediate> partitioner;
    // the reducer function
    private ReduceFunction<KIntermediate, VIntermediate, VOut> reducer;

    /**
     * Constructs a new partition-reduce job given input and appropriate
     * partitioner and reducer types.
     *
     * @param i the input dataset.
     * @param p the partitioner function.
     * @param r the reducer function.
     */
    public PartitionReduceJob(Dataset<TIn> i,
                              PartitionFunction<TIn,
                                      KIntermediate, VIntermediate> p,
                              ReduceFunction<KIntermediate, VIntermediate,
                                      VOut> r) {
        input = i;
        partitioner = p;
        reducer = r;
    }

    /**
     * Executes the job over a pool of execution.
     *
     * @param pool the execution pool.
     * @return the output of the job (i.e., the output of the reduce step).
     */
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

    /**
     * Debug main.
     *
     * @param args parameters.
     */
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
                            for (; v.hasNext();) {
                                sb.append(v.next()).append("_");
                            }
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
