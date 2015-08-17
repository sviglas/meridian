package net.sviglas.meridian.task;

import net.sviglas.util.Pair;

public abstract class PartitionFunction<TIn,
        KOut extends Comparable<? super KOut>, VOut> {
    private Class<TIn> inputType;
    private Class<KOut> outputKeyType;
    private Class<VOut> outputValueType;

    public PartitionFunction(Class<TIn> tin, Class<KOut> kout,
                             Class<VOut> vout) {
        inputType = tin;
        outputKeyType = kout;
        outputValueType = vout;
    }

    public Class<TIn> getInputType() { return inputType; }

    public Class<KOut> getOutputKeyType() { return outputKeyType; }

    public Class<VOut> getOutputValueType() { return outputValueType; }

    public abstract Pair<KOut, VOut> partition(TIn t);
}
