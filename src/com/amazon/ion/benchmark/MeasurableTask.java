package com.amazon.ion.benchmark;

import java.io.IOException;
import java.util.concurrent.Callable;

public interface MeasurableTask {
    void setUpTrial() throws IOException;
    void setUpIteration() throws IOException;
    void tearDownIteration() throws IOException;
    Callable<Void> getTask();
}
