package com.amazon.ion.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

abstract class MeasurableWriteTask<T> implements MeasurableTask {

    @FunctionalInterface
    interface WriteInstruction<T> {
        void execute(T writer) throws IOException;
    }

    private final List<WriteInstruction<T>> writeInstructions = new ArrayList<>();
    final Path inputPath;
    final WriteOptionsCombination options;
    private T writer = null;
    private Path currentFile = null;
    private ByteArrayOutputStream currentBuffer = null;

    MeasurableWriteTask(Path inputPath, WriteOptionsCombination options) {
        this.inputPath = inputPath;
        this.options = options;
    }

    abstract void generateWriteInstructionsDom(Consumer<WriteInstruction<T>> instructionsSink) throws IOException;
    abstract void generateWriteInstructionsStreaming(Consumer<WriteInstruction<T>> instructionsSink) throws IOException;
    abstract T newWriter() throws IOException;
    abstract void closeWriter(T writer) throws IOException;

    OutputStream newOutputStream() throws IOException {
        OutputStream out = null;
        switch (options.ioType) {
            case BUFFER:
                currentBuffer = new ByteArrayOutputStream();
                out = currentBuffer;
                break;
            case FILE:
                currentFile = TemporaryFiles.newTempFile(inputPath.toFile().getName(), options.format.getSuffix());
                out = options.newOutputStream(currentFile.toFile());
                break;
        }
        return out;
    }

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
    public void setUpIteration() throws IOException {
        writer = newWriter();
    }

    @Override
    public void tearDownIteration() throws IOException {
        closeWriter(writer);
        long serializedSize = 0;
        if (currentFile != null) {
            serializedSize = currentFile.toFile().length();
            Files.delete(currentFile);
            currentFile = null;
        } else if (currentBuffer != null) {
            serializedSize = currentBuffer.size();
            currentBuffer = null;
        }
        SerializedSizeProfiler.setSize(serializedSize);
    }

    @Override
    public final Callable<Void> getTask() {
        return () -> {
            for (WriteInstruction<T> instruction : writeInstructions) {
                instruction.execute(writer);
            }
            return null;
        };
    }
}
