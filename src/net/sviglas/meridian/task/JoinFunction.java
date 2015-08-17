package net.sviglas.meridian.task;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 10/08/15.
 */
public abstract class JoinFunction<Tl, Tr, TOut> {
    private Class<Tl> leftInputType;
    private Class<Tr> rightInputType;
    private Class<TOut> outputType;

    public JoinFunction(Class<Tl> tl, Class<Tr> tr, Class<TOut> tout) {
        leftInputType = tl;
        rightInputType = tr;
        outputType = tout;
    }

    public Class<Tl> getLeftInputType() { return leftInputType; }

    public Class<Tr> getRightInputType() { return rightInputType; }

    public Class<TOut> getOutputValueType() { return outputType; }

    public abstract boolean equal(Tl l, Tr r);
    public abstract TOut combine(Tl l, Tr r);
}
