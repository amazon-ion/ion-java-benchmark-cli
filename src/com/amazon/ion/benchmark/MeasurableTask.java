package com.amazon.ion.benchmark;

import java.io.IOException;

/**
 * A task to benchmark.
 */
public interface MeasurableTask {

    @FunctionalInterface
    interface Task {
        void run(SideEffectConsumer consumer) throws Exception;
    }

    /**
     * Set up the trial, which is the complete set of iterations.
     * @throws IOException if thrown during setup.
     */
    void setUpTrial() throws IOException;

    /**
     * Tear down the trial.
     * @throws IOException if thrown during teardown.
     */
    void tearDownTrial() throws IOException;

    /**
     * Set up an iteration, of which there are many per trial.
     * @throws IOException if thrown during setup.
     */
    void setUpIteration() throws IOException;

    /**
     * Tear down an iteration.
     * @throws IOException if thrown during teardown.
     */
    void tearDownIteration() throws IOException;

    /**
     * @return the piece of code to benchmark.
     */
    Task getTask();
}
