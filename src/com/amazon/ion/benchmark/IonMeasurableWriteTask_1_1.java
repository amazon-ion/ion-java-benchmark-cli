package com.amazon.ion.benchmark;

import com.amazon.ion.MacroAwareIonWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * A MeasurableWriteTask for writing data in the Ion 1.1+ format (either text or binary) using input data that is
 * also in the Ion 1.1+ format. This ensures encoding directives and macro invocations are preserved, providing
 * a more accurate measurement of the write performance of the input. In cases where either the input or the output
 * is Ion 1.0, {@link IonMeasurableWriteTask} should be used instead, as encoding directives and macro invocations
 * cannot be preserved in that case.
 */
public class IonMeasurableWriteTask_1_1 extends MeasurableWriteTask<MacroAwareIonWriter> {

    private final IonUtilities.IonWriterSupplier writerBuilder;

    /**
     * @param inputPath path to the data to re-write.
     * @param options options to use when writing.
     * @throws IOException if thrown when handling the options.
     */
    IonMeasurableWriteTask_1_1(Path inputPath, WriteOptionsCombination options) throws IOException {
        super(inputPath, options);
        if (options.format == Format.ION_TEXT) {
            writerBuilder = IonUtilities.newTextWriterSupplier(options);
        } else if (options.format == Format.ION_BINARY) {
            writerBuilder = IonUtilities.newBinaryWriterSupplier(options);
        } else {
            throw new IllegalStateException("IonFormatWriter is compatible only with ION_TEXT and ION_BINARY");
        }
    }

    @Override
    void generateWriteInstructionsDom(Consumer<WriteInstruction<MacroAwareIonWriter>> instructionsSink) {
        throw new UnsupportedOperationException("Write benchmarking of Ion 1.1 from the DOM is not yet supported.");
    }

    @Override
    void generateWriteInstructionsStreaming(Consumer<WriteInstruction<MacroAwareIonWriter>> instructionsSink) throws IOException {
        IonUtilities.rewriteIon11File(inputFile, options, new RecordingMacroAwareIonWriter(instructionsSink));
    }

    @Override
    MacroAwareIonWriter newWriter(OutputStream outputStream) throws IOException {
        return (MacroAwareIonWriter) writerBuilder.get(outputStream);
    }

    @Override
    void closeWriter(MacroAwareIonWriter writer) throws IOException {
        // Note: this closes the underlying OutputStream.
        writer.close();
    }
}
