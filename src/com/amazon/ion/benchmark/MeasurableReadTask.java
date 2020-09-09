package com.amazon.ion.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A MeasurableTask for read benchmarks.
 */
abstract class MeasurableReadTask implements MeasurableTask {

    final File inputFile;
    final ReadOptionsCombination options;
    byte[] buffer = null;

    /**
     * @param inputPath path to the data to read.
     * @param options options to use when reading.
     */
    MeasurableReadTask(Path inputPath, ReadOptionsCombination options) {
        this.inputFile = inputPath.toFile();
        this.options = options;
    }

    /**
     * Initialize the reader and perform a fully-materialized deep read of the data from a byte buffer. The "reader"
     * is defined as any context that is tied to a single stream. Context that is reused across arbitrarily-many streams
     * may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyTraverseFromBuffer() throws IOException;

    /**
     * Initialize the reader and perform a fully-materialized deep read of the data from a file. The "reader"
     * is defined as any context that is tied to a single stream. Context that is reused across arbitrarily-many streams
     * may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyTraverseFromFile() throws IOException;

    /**
     * Initialize the reader and perform a sparse read of the data from a byte buffer, only materializing the values at
     * the specified paths. The "reader" is defined as any context that is tied to a single stream. Context that is
     * reused across arbitrarily-many streams may be initialized outside of the timed block in
     * {@link #setUpIteration()}.
     * @param paths the paths of values to materialize.
     * @throws IOException if thrown during reading.
     */
    abstract void traverseFromBuffer(List<String> paths) throws IOException;

    /**
     * Initialize the reader and perform a sparse read of the data from a file, only materializing the values at the
     * specified paths. The "reader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @param paths the paths of values to materialize.
     * @throws IOException if thrown during reading.
     */
    abstract void traverseFromFile(List<String> paths) throws IOException;

    /**
     * Initialize the loader and perform a fully-materialized deep read of the data from a byte buffer into a DOM
     * representation. The "loader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromBuffer() throws IOException;

    /**
     * Initialize the loader and perform a fully-materialized deep read of the data from a file into a DOM
     * representation. The "loader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromFile() throws IOException;

    @Override
    public void setUpTrial() throws IOException {
        if (options.ioType == IoType.BUFFER) {
            // Note: the input file will already have been truncated to the value limit, if necessary.
            buffer = Files.readAllBytes(inputFile.toPath());
        }
        SerializedSizeProfiler.setSize(inputFile.length());
    }

    @Override
    public final Callable<Void> getTask() {
        if (options.paths != null) {
            if (buffer != null) {
                return () -> {
                    traverseFromBuffer(options.paths);
                    return null;
                };
            } else {
                return () -> {
                    traverseFromFile(options.paths);
                    return null;
                };
            }
        } else if (options.api == IonAPI.STREAMING) {
            if (buffer != null) {
                return () -> {
                    fullyTraverseFromBuffer();
                    return null;
                };
            } else {
                return () -> {
                    fullyTraverseFromFile();
                    return null;
                };
            }
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
            throw new IllegalStateException("Illegal combination of options.");
        }
    }
}
