package net.sviglas.meridian.task;

public interface ForFunction <RIn, ROut> {
    ROut apply(RIn r);
}
