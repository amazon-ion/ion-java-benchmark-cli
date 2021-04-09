package com.amazon.ion.benchmark;

interface SideEffectConsumer {

    SideEffectConsumer NO_OP = new SideEffectConsumer() {

        @Override
        public void consume(boolean b) {
            // Do nothing.
        }

        @Override
        public void consume(int i) {
            // Do nothing.
        }

        @Override
        public void consume(long l) {
            // Do nothing.
        }

        @Override
        public void consume(float f) {
            // Do nothing.
        }

        @Override
        public void consume(double d) {
            // Do nothing.
        }

        @Override
        public void consume(Object o) {
            // Do nothing.
        }
    };

    void consume(boolean b);
    void consume(int i);
    void consume(long l);
    void consume(float f);
    void consume(double d);
    void consume(Object o);
}
