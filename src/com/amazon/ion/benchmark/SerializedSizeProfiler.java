package com.amazon.ion.benchmark;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.ArrayList;
import java.util.Collection;

/**
 * JMH Profiler plugin to measure the size of either the input data (for read benchmarks) or the output data (for write
 * benchmarks).
 */
public class SerializedSizeProfiler implements InternalProfiler {

    private static long size;

    /**
     * @param size sets the size to be reported by the SerializedSizeProfiler.
     */
    static void setSize(long size) {
        SerializedSizeProfiler.size = size;
    }

    @Override
    public String getDescription() {
        return "Serialized size profiler";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        // Nothing
    }

    @Override
    public Collection<? extends Result> afterIteration(
        BenchmarkParams benchmarkParams,
        IterationParams iterationParams,
        IterationResult iterationResult
    ) {
        Collection<Result> results = new ArrayList<>();
        results.add(new ScalarResult("Serialized size", size / 1e6, "MB", AggregationPolicy.MAX));

        return results;
    }
}
