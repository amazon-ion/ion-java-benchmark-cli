package com.amazon.ion.benchmark;

import org.openjdk.jmh.infra.Blackhole;

public class BlackholeSideEffectConsumer implements SideEffectConsumer {

    private final Blackhole blackhole;

    BlackholeSideEffectConsumer(Blackhole blackhole) {
        this.blackhole = blackhole;
    }

    @Override
    public void consume(boolean b) {
        blackhole.consume(b);
    }

    @Override
    public void consume(int i) {
        blackhole.consume(i);
    }

    @Override
    public void consume(long l) {
        blackhole.consume(l);
    }

    @Override
    public void consume(float f) {
        blackhole.consume(f);
    }

    @Override
    public void consume(double d) {
        blackhole.consume(d);
    }

    @Override
    public void consume(Object o) {
        blackhole.consume(o);
    }
}
