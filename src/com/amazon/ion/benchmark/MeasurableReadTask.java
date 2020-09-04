package com.amazon.ion.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A MeasurableTask for read benchmarks.
 */
abstract class MeasurableReadTask implements MeasurableTask {

    final Path inputPath;
    final ReadOptionsCombination options;
    byte[] buffer = null;

    /**
     * @param inputPath path to the data to read.
     * @param options options to use when reading.
     */
    MeasurableReadTask(Path inputPath, ReadOptionsCombination options) {
        this.inputPath = inputPath;
        this.options = options;
    }

    /**
     * Perform a fully-materialized deep read of the data.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyTraverse() throws IOException;

    /**
     * Perform a sparse read of the data, only materializing the values at the specified paths.
     * @param paths the paths of values to materialize.
     * @throws IOException if thrown during reading.
     */
    abstract void traverse(List<String> paths) throws IOException;

    /**
     * Perform a fully-materialized read of the data from a byte buffer into a DOM representation.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromBuffer() throws IOException;

    /**
     * Perform a fully-materialized read of the data from a file into a DOM representation.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromFile() throws IOException;

    @Override
    public void setUpTrial() throws IOException {
        if (options.ioType == IoType.BUFFER) {
            // Note: the input file will already have been truncated to the value limit, if necessary.
            buffer = Files.readAllBytes(inputPath);
        }
        SerializedSizeProfiler.setSize(inputPath.toFile().length());
    }

    @Override
    public final Callable<Void> getTask() {
        if (options.paths != null) {
            return () -> {
                traverse(options.paths);
                return null;
            };
        } else if (options.api == IonAPI.STREAMING) {
            return () -> {
                fullyTraverse();
                return null;
            };
        } else if (options.api == IonAPI.DOM) {
            if (buffer != null) {
                return () -> {
                    fullyReadDomFromBuffer();
                    return null;
                };
            } else {
                return () -> {
                    fullyReadDomFromFile();
                    return null;
                };
            }
        } else {
            throw new IllegalStateException("Illegal combination of arguments.");
        }
    }
}
