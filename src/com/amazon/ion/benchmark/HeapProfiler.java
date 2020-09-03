package com.amazon.ion.benchmark;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;

public class HeapProfiler implements InternalProfiler {

    @Override
    public String getDescription() {
        return "Heap usage profiler";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        // Nothing.
    }

    @Override
    public Collection<? extends Result> afterIteration(
        BenchmarkParams benchmarkParams,
        IterationParams iterationParams,
        IterationResult result
    ) {
        long totalHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

        Collection<Result> results = new ArrayList<>();
        results.add(new ScalarResult("Heap usage", totalHeap / 1e6, "MB", AggregationPolicy.AVG));

        return results;
    }
}
