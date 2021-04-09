package com.amazon.ion.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * JMH benchmark for a single options combination.
 */
@State(Scope.Benchmark)
public class Bench {

    /**
     * Name of the input file.
     */
    @Param({""})
    private String input;

    /**
     * Serialized options combination.
     */
    @Param({"{}"})
    private String options;

    MeasurableTask measurableTask = null;
    MeasurableTask.Task taskToMeasure = null;

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        OptionsCombinationBase optionsCombination = OptionsCombinationBase.from(options);
        measurableTask = optionsCombination.createMeasurableTask(Paths.get(input));
        measurableTask.setUpTrial();
        taskToMeasure = measurableTask.getTask();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        measurableTask.tearDownTrial();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() throws IOException {
        measurableTask.setUpIteration();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        measurableTask.tearDownIteration();
    }

    @Benchmark
    public void run(Blackhole blackhole) throws Exception {
        taskToMeasure.run(new BlackholeSideEffectConsumer(blackhole));
    }
}
