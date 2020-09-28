package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

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
            if (reader.isInStruct()) {
                if (options.useSymbolTokens) {
                    SymbolToken fieldName = reader.getFieldNameSymbol();
                    instructionsSink.accept(writer -> writer.setFieldNameSymbol(fieldName));
                } else {
                    String fieldName = reader.getFieldName();
                    instructionsSink.accept(writer -> writer.setFieldName(fieldName));
                }
            }
            if (options.useSymbolTokens) {
                SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
                if (annotations.length > 0) {
                    instructionsSink.accept(writer -> writer.setTypeAnnotationSymbols(annotations));
                }
            } else {
                String[] annotations = reader.getTypeAnnotations();
                if (annotations.length > 0) {
                    instructionsSink.accept(writer -> writer.setTypeAnnotations(annotations));
                }
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
                        if (options.useSymbolTokens) {
                            SymbolToken symbolValue = reader.symbolValue();
                            instructionsSink.accept(writer -> writer.writeSymbolToken(symbolValue));
                        } else {
                            String symbolValue = reader.stringValue();
                            instructionsSink.accept(writer -> writer.writeSymbol(symbolValue));
                        }
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
            if (reader.getDepth() == 0) {
                numberOfTopLevelValues++;
                if (options.flushPeriod != null && numberOfTopLevelValues % options.flushPeriod == 0) {
                    instructionsSink.accept(IonWriter::flush);
                }
                if (options.limit != Integer.MAX_VALUE && numberOfTopLevelValues >= options.limit) {
                    break;
                }
            }
        }
    }

    @Override
    void generateWriteInstructionsStreaming(Consumer<WriteInstruction<IonWriter>> instructionsSink) throws IOException {
        try (IonReader reader = IonUtilities.newReaderBuilderForInput(options).build(options.newInputStream(inputFile))) {
            fullyTraverse(reader, instructionsSink);
            instructionsSink.accept(IonWriter::finish);
        }
    }

    @Override
    void generateWriteInstructionsDom(Consumer<WriteInstruction<IonWriter>> instructionsSink) throws IOException {
        IonDatagram datagram;
        if (options.limit == Integer.MAX_VALUE) {
            datagram = IonUtilities.ionSystemForInput(options).getLoader().load(inputFile);
        } else {
            datagram = ION_SYSTEM.newDatagram();
            try (IonReader reader = IonUtilities.newReaderBuilderForInput(options).build(options.newInputStream(inputFile))) {
                Iterator<IonValue> valueIterator = ION_SYSTEM.iterate(reader);
                while (valueIterator.hasNext()) {
                    datagram.add(valueIterator.next());
                    if (datagram.size() >= options.limit) {
                        break;
                    }
                }
            }
        }
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
