package com.amazon.ion.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * A MeasurableTask for read benchmarks.
 */
abstract class MeasurableReadTask implements MeasurableTask {

    final Path originalFile;
    File inputFile;
    final ReadOptionsCombination options;
    byte[] buffer = null;

    /**
     * @param inputPath path to the data to read.
     * @param options options to use when reading.
     * @throws IOException if thrown when validating the options against the input.
     */
    MeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        this.originalFile = inputPath;
        this.options = options;
        if (Format.classify(originalFile).isIon()
            && !IonUtilities.importsEqual(options.importsForInputFile, originalFile.toFile())) {
            throw new IllegalArgumentException(
                "The input file contains shared symbol table imports. Those imports must be " +
                    "supplied using --ion-imports-for-input."
            );
        }
    }

    /**
     * Initialize the reader and perform a fully-materialized deep read of the data from a byte buffer. The "reader"
     * is defined as any context that is tied to a single stream. Context that is reused across arbitrarily-many streams
     * may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyTraverseFromBuffer(SideEffectConsumer consumer) throws IOException;

    /**
     * Initialize the reader and perform a fully-materialized deep read of the data from a file. The "reader"
     * is defined as any context that is tied to a single stream. Context that is reused across arbitrarily-many streams
     * may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException;

    /**
     * Initialize the reader and perform a sparse read of the data from a byte buffer, only materializing the values at
     * the specified paths. The "reader" is defined as any context that is tied to a single stream. Context that is
     * reused across arbitrarily-many streams may be initialized outside of the timed block in
     * {@link #setUpIteration()}.
     * @param paths the paths of values to materialize.
     * @throws IOException if thrown during reading.
     */
    abstract void traverseFromBuffer(List<String> paths, SideEffectConsumer consumer) throws IOException;

    /**
     * Initialize the reader and perform a sparse read of the data from a file, only materializing the values at the
     * specified paths. The "reader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @param paths the paths of values to materialize.
     * @throws IOException if thrown during reading.
     */
    abstract void traverseFromFile(List<String> paths, SideEffectConsumer consumer) throws IOException;

    /**
     * Initialize the loader and perform a fully-materialized deep read of the data from a byte buffer into a DOM
     * representation. The "loader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromBuffer(SideEffectConsumer consumer) throws IOException;

    /**
     * Initialize the loader and perform a fully-materialized deep read of the data from a file into a DOM
     * representation. The "loader" is defined as any context that is tied to a single stream. Context that is reused
     * across arbitrarily-many streams may be initialized outside of the timed block in {@link #setUpIteration()}.
     * @throws IOException if thrown during reading.
     */
    abstract void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException;

    @Override
    public void setUpTrial() throws IOException {
        inputFile = options.convertFileIfNecessary(originalFile).toFile();
        if (options.ioType == IoType.BUFFER) {
            // Note: the input file will already have been truncated to the value limit, if necessary.
            buffer = Files.readAllBytes(inputFile.toPath());
        }
        SerializedSizeProfiler.setSize(inputFile.length());
    }

    @Override
    public void tearDownTrial() throws IOException {
        if (!inputFile.equals(originalFile.toFile())) {
            // 'inputFile' is a temporary file that was converted form 'originalFile' for this specific options
            // combination. Clean it up to avoid consuming this disk space throughout the rest of the trials.
            Files.delete(inputFile.toPath());
        }
        buffer = null;
    }

    @Override
    public final Task getTask() {
        if (options.paths != null) {
            if (buffer != null) {
                return (consumer) -> traverseFromBuffer(options.paths, consumer);
            } else {
                return (consumer) -> traverseFromFile(options.paths, consumer);
            }
        } else if (options.api == API.STREAMING) {
            if (buffer != null) {
                return this::fullyTraverseFromBuffer;
            } else {
                return this::fullyTraverseFromFile;
            }
        } else if (options.api == API.DOM) {
            if (buffer != null) {
                return this::fullyReadDomFromBuffer;
            } else {
                return this::fullyReadDomFromFile;
            }
        } else {
            throw new IllegalStateException("Illegal combination of options.");
        }
    }
}
