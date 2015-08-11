package net.sviglas.meridian;

import java.util.List;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.SortedSet;

import java.util.concurrent.ForkJoinPool;

import net.sviglas.util.Pair;

public class MapReduceJob <
    Record
    , KInput
    , VInput
    , KOutput
    , VIntermediate
    , VOutput> {
/*
    private List<Record> input;
    private Extractor<Record, KInput, VInput> extractor;
    private PartitionFunction<KInput, VInput, KOutput, VIntermediate> partitionFunction;
    private ReduceFunction<KOutput, VIntermediate, VOutput> reduceFunction;

    public MapReduceJob(List<Record> i,
                        Extractor<Record, KInput, VInput> e,
                        PartitionFunction<KInput, VInput, KOutput, VIntermediate> m,
                        ReduceFunction<KOutput, VIntermediate, VOutput> r) {
        input = i;
        extractor = e;
        partitionFunction = m;
        reduceFunction = r;
    }
                        

    public LList<Pair<KOutput, VOutput>> execute(ForkJoinPool pool) {
        PartitionTask<Record, KInput, VInput, KOutput, VIntermediate> maptask =
            new PartitionTask<>(input, new IndexRange(0, input.size()),
                          extractor, partitionFunction);
        SortedMap<KOutput, LList<VIntermediate>> mapout =
            pool.invoke(maptask);
        KeyRange<KOutput> kr =
            new KeyRange<KOutput>((SortedSet<KOutput>) mapout.keySet());
        ReduceTask<KOutput, VIntermediate, VOutput> rtask =
            new ReduceTask<>(mapout, kr, reduceFunction);
        return pool.invoke(rtask);
    }

    public static void main(String [] args) {
        try {
            ForkJoinPool forkJoinPool = new ForkJoinPool();
            
            int limit = 100;
            List<Pair<Integer, String>> list =
                new ArrayList<>();
            for (int i = 0; i < limit; i++) {
                Pair<Integer, String> pair =
                    new Pair<>(i % 10, "j" + (i*i));
                list.add(pair);
            }

            Identity<Integer, String> identity = new Identity<>();
            MapReduceJob<Pair<Integer, String>, Integer, String, Integer, String, String>
                job = new MapReduceJob<>(list, identity, identity, identity);
            LList<Pair<Integer, String>> rout = job.execute(forkJoinPool);
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
