package net.sviglas.meridian.task;

import java.util.Iterator;

public abstract class ReduceFunction <KIn, VIn, VOut> {
    private Class<KIn> inputKeyType;
    private Class<VIn> inputValueType;
    private Class<VOut> outputValueType;

    public ReduceFunction(Class<KIn> kin, Class<VIn> vin, Class<VOut> vout) {
        inputKeyType = kin;
        inputValueType = vin;
        outputValueType = vout;
    }

    public Class<KIn> getInputKeyType() { return inputKeyType; }

    public Class<VIn> getInputValueType() { return inputValueType; }

    public Class<VOut> getOutputValueType() { return outputValueType; }

    public abstract VOut reduce(KIn k, Iterator<VIn> v);
}
