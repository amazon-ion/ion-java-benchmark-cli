package com.amazon.ion.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

abstract class MeasurableReadTask implements MeasurableTask {

    final Path inputPath;
    final ReadOptionsCombination parameters;
    byte[] buffer = null;

    MeasurableReadTask(Path inputPath, ReadOptionsCombination parameters) {
        this.inputPath = inputPath;
        this.parameters = parameters;
    }

    abstract void fullyTraverse() throws IOException;
    abstract void traverse(List<String> paths) throws IOException;
    abstract void fullyReadDomFromBuffer() throws IOException;
    abstract void fullyReadDomFromFile() throws IOException;

    @Override
    public void setUpTrial() throws IOException {
        if (parameters.ioType == IoType.BUFFER) {
            // Note: the input file will already have been truncated to the value limit, if necessary.
            buffer = Files.readAllBytes(inputPath);
        }
        SerializedSizeProfiler.setSize(inputPath.toFile().length());
    }

    @Override
    public final Callable<Void> getTask() {
        if (parameters.paths != null) {
            return () -> {
                traverse(parameters.paths);
                return null;
            };
        } else if (parameters.api == IonAPI.STREAMING) {
            return () -> {
                fullyTraverse();
                return null;
            };
        } else if (parameters.api == IonAPI.DOM) {
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
