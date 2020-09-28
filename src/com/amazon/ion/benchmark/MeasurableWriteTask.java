package com.amazon.ion.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * A MeasurableTask for write benchmarks.
 */
abstract class MeasurableWriteTask<T> implements MeasurableTask {

    /**
     * A single instruction to execute. The measurable task is composed of a sequence of instructions.
     * @param <T> type of the context needed by the WriteInstruction. For IonJava, this is an IonWriter.
     */
    @FunctionalInterface
    interface WriteInstruction<T> {
        void execute(T writer) throws IOException;
    }

    private final List<WriteInstruction<T>> writeInstructions = new ArrayList<>();
    final File inputFile;
    final WriteOptionsCombination options;
    File currentFile = null;
    ByteArrayOutputStream currentBuffer = null;

    /**
     * @param inputPath path to the data to write.
     * @param options options to use while writing.
     */
    MeasurableWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
        this.inputFile = inputPath.toFile();
        this.options = options;
        if (Format.classify(inputPath).isIon()
            && !IonUtilities.importsEqual(options.importsForInputFile, inputPath.toFile())) {
            throw new IllegalArgumentException(
                "The input file contains shared symbol table imports. Those imports must be " +
                    "supplied using --ion-imports-for-input."
            );
        }
    }

    /**
     * Generate a sequence of WriteInstructions that re-write the input file with the configured options using a DOM
     * API.
     * @param instructionsSink the sink for the sequence of generated WriteInstructions.
     * @throws IOException if thrown when generating WriteInstructions.
     */
    abstract void generateWriteInstructionsDom(Consumer<WriteInstruction<T>> instructionsSink) throws IOException;

    /**
     * Generate a sequence of WriteInstructions that re-write the input file with the configured options using a
     * streaming API.
     * @param instructionsSink the sink for the sequence of generated WriteInstructions.
     * @throws IOException if thrown when generating WriteInstructions.
     */
    abstract void generateWriteInstructionsStreaming(Consumer<WriteInstruction<T>> instructionsSink) throws IOException;

    /**
     * @return a new writer context instance.
     * @param outputStream the OutputStream to which the new writer will write.
     * @throws IOException if thrown during construction of the context.
     */
    abstract T newWriter(OutputStream outputStream) throws IOException;

    /**
     * Close a given writer context instance and its underlying OutputStream.
     * @param writer the context to be closed.
     * @throws IOException if thrown while closing the context.
     */
    abstract void closeWriter(T writer) throws IOException;

    @Override
    public void setUpTrial() throws IOException {
        writeInstructions.clear();
        switch (options.api) {
            case STREAMING:
                generateWriteInstructionsStreaming(writeInstructions::add);
                break;
            case DOM:
                generateWriteInstructionsDom(writeInstructions::add);
                break;
        }
    }

    @Override
    public void tearDownTrial() {
        writeInstructions.clear();
    }

    @Override
    public void setUpIteration() throws IOException {
        if (options.ioType == IoType.FILE) {
            currentFile = TemporaryFiles.newTempFile(inputFile.getName(), options.format.getSuffix()).toFile();
        }
    }

    @Override
    public void tearDownIteration() throws IOException {
        long serializedSize = 0;
        if (currentFile != null) {
            serializedSize = currentFile.length();
            Files.delete(currentFile.toPath());
            currentFile = null;
        } else if (currentBuffer != null) {
            serializedSize = currentBuffer.size();
            currentBuffer = null;
        }
        SerializedSizeProfiler.setSize(serializedSize);
    }

    @Override
    public final Callable<Void> getTask() {
        switch (options.ioType) {
            case BUFFER:
                return () -> {
                    currentBuffer = new ByteArrayOutputStream();
                    T writer = newWriter(currentBuffer);
                    for (WriteInstruction<T> instruction : writeInstructions) {
                        instruction.execute(writer);
                    }
                    closeWriter(writer);
                    return null;
                };
            case FILE:
                return () -> {
                    T writer = newWriter(options.newOutputStream(currentFile));
                    for (WriteInstruction<T> instruction : writeInstructions) {
                        instruction.execute(writer);
                    }
                    closeWriter(writer);
                    return null;
                };
            default:
                throw new IllegalStateException("Write support missing for IO type " + options.ioType);
        }
    }
}
