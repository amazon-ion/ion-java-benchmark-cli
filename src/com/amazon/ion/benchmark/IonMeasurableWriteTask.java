package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;

import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;

/**
 * A MeasurableWriteTask for writing data in the Ion format (either text or binary).
 */
class IonMeasurableWriteTask extends MeasurableWriteTask<IonWriter> {

    private final IonUtilities.IonWriterSupplier writerBuilder;

    /**
     * @param inputPath path to the data to re-write.
     * @param options options to use when writing.
     * @throws IOException if thrown when handling the options.
     */
    IonMeasurableWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
        super(inputPath, options);
        if (options.format == Format.ION_TEXT) {
            writerBuilder = IonUtilities.newTextWriterSupplier(options);
        } else if (options.format == Format.ION_BINARY) {
            writerBuilder = IonUtilities.newBinaryWriterSupplier(options);
        } else {
            throw new IllegalStateException("IonFormatWriter is compatible only with ION_TEXT and ION_BINARY");
        }
    }

    /**
     * Generate WriteInstructions by fully traversing the input data.
     * @param reader IonReader over the input data.
     * @param instructionsSink sink for the generated WriteInstructions.
     */
    private void fullyTraverse(IonReader reader, Consumer<WriteInstruction<IonWriter>> instructionsSink) {
        int numberOfTopLevelValues = 0;
        while (reader.next() != null) {
            if (reader.getDepth() == 0) {
                numberOfTopLevelValues++;
                if (options.flushPeriod != null && numberOfTopLevelValues % options.flushPeriod == 0) {
                    instructionsSink.accept(IonWriter::flush);
                }
            }
            if (reader.isInStruct()) {
                String fieldName = reader.getFieldName();
                instructionsSink.accept(writer -> writer.setFieldName(fieldName));
            }
            Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
            while (annotationsIterator.hasNext()) {
                String annotation = annotationsIterator.next();
                instructionsSink.accept(writer -> writer.addTypeAnnotation(annotation));
            }
            IonType type = reader.getType();
            if (reader.isNullValue()) {
                instructionsSink.accept(writer -> writer.writeNull(type));
            } else {
                switch (type) {
                    case NULL:
                        instructionsSink.accept(IonWriter::writeNull);
                        break;
                    case BOOL:
                        boolean boolValue = reader.booleanValue();
                        instructionsSink.accept(writer -> writer.writeBool(boolValue));
                        break;
                    case INT:
                        switch (reader.getIntegerSize()) {
                            case INT:
                                int intValue = reader.intValue();
                                instructionsSink.accept(writer -> writer.writeInt(intValue));
                                break;
                            case LONG:
                                long longValue = reader.longValue();
                                instructionsSink.accept(writer -> writer.writeInt(longValue));
                                break;
                            case BIG_INTEGER:
                                BigInteger bigIntegerValue = reader.bigIntegerValue();
                                instructionsSink.accept(writer -> writer.writeInt(bigIntegerValue));
                                break;
                        }
                        break;
                    case FLOAT:
                        double doubleValue = reader.doubleValue();
                        // TODO 32-bit floats
                        instructionsSink.accept(writer -> writer.writeFloat(doubleValue));
                        break;
                    case DECIMAL:
                        BigDecimal decimalValue = reader.decimalValue();
                        instructionsSink.accept(writer -> writer.writeDecimal(decimalValue));
                        break;
                    case TIMESTAMP:
                        Timestamp timestampValue = reader.timestampValue();
                        instructionsSink.accept(writer -> writer.writeTimestamp(timestampValue));
                        break;
                    case SYMBOL:
                        String symbolValue = reader.stringValue();
                        instructionsSink.accept(writer -> writer.writeSymbol(symbolValue));
                        break;
                    case STRING:
                        String stringValue = reader.stringValue();
                        instructionsSink.accept(writer -> writer.writeString(stringValue));
                        break;
                    case CLOB:
                        byte[] clobValue = reader.newBytes();
                        instructionsSink.accept(writer -> writer.writeClob(clobValue));
                        break;
                    case BLOB:
                        byte[] blobValue = reader.newBytes();
                        instructionsSink.accept(writer -> writer.writeBlob(blobValue));
                        break;
                    case LIST:
                    case SEXP:
                    case STRUCT:
                        reader.stepIn();
                        instructionsSink.accept(writer -> writer.stepIn(type));
                        fullyTraverse(reader, instructionsSink);
                        reader.stepOut();
                        instructionsSink.accept(IonWriter::stepOut);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    void generateWriteInstructionsStreaming(Consumer<WriteInstruction<IonWriter>> instructionsSink) throws IOException {
        try (IonReader reader = IonReaderBuilder.standard().build(options.newInputStream(inputFile))) {
            fullyTraverse(reader, instructionsSink);
            instructionsSink.accept(IonWriter::finish);
        }
    }

    @Override
    void generateWriteInstructionsDom(Consumer<WriteInstruction<IonWriter>> instructionsSink) throws IOException {
        IonDatagram datagram = ION_SYSTEM.getLoader().load(inputFile);
        instructionsSink.accept(datagram::writeTo);
        instructionsSink.accept(IonWriter::finish);
    }

    @Override
    public IonWriter newWriter(OutputStream outputStream) throws IOException {
        return writerBuilder.get(outputStream);
    }

    @Override
    public void closeWriter(IonWriter writer) throws IOException {
        // Note: this closes the underlying OutputStream.
        writer.close();
    }

}
