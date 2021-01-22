package com.amazon.ion.benchmark;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionpathextraction.PathExtractor;
import com.amazon.ionpathextraction.PathExtractorBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * A MeasurableReadTask for reading data in the Ion format (either text or binary).
 */
class IonMeasurableReadTask extends MeasurableReadTask {

    private static final int DEFAULT_INCREMENTAL_BUFFER_SIZE = 64 * 1024;
    private static final int DEFAULT_REUSABLE_LOB_BUFFER_SIZE = 1024;
    private final IonReaderBuilder readerBuilder;
    private final PathExtractor<?> pathExtractor;
    private final IonSystem ionSystem;
    private final byte[] reusableLobBuffer;

    /**
     * Returns the next power of two greater than or equal to the given value.
     * @param value the start value.
     * @return the next power of two.
     */
    private static int nextPowerOfTwo(int value) {
        return (int) Math.pow(2, Math.ceil(Math.log10(value) / Math.log10(2)));
    }

    /**
     * Callback function for path extractor matches. Fully consumes the current value.
     * @param reader the reader positioned at the match.
     * @return 0, meaning that the reader should not step out of the current container after a match.
     */
    private int pathExtractorCallback(IonReader reader) {
        consumeCurrentValue(reader, reader.isInStruct());
        return 0;
    }

    /**
     * @param inputPath the Ion data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    IonMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        ionSystem = IonUtilities.ionSystemForBenchmark(options);
        readerBuilder = IonUtilities.newReaderBuilderForBenchmark(options)/*.
            withIncrementalReadingEnabled(options.readerType == IonReaderType.INCREMENTAL)*/;
        /*
        if (readerBuilder.isIncrementalReadingEnabled()) {
            if (options.initialBufferSize != null) {
                readerBuilder.withBufferConfiguration(
                    new IonBufferConfiguration().withInitialBufferSize(options.initialBufferSize)
                );
            } else {
                long inputSize = inputPath.toFile().length();
                if (inputSize < DEFAULT_INCREMENTAL_BUFFER_SIZE) {
                    readerBuilder.withBufferConfiguration(
                        new IonBufferConfiguration().withInitialBufferSize(nextPowerOfTwo((int) inputSize))
                    );
                }
            }
        }
         */
        if (options.paths != null) {
            PathExtractorBuilder<?> pathExtractorBuilder = PathExtractorBuilder.standard();
            for (String path : options.paths) {
                pathExtractorBuilder.withSearchPath(path, this::pathExtractorCallback);
            }
            pathExtractor = pathExtractorBuilder.build();
        } else {
            pathExtractor = null;
        }
        if (options.useLobChunks) {
            reusableLobBuffer = new byte[DEFAULT_REUSABLE_LOB_BUFFER_SIZE];
        } else {
            reusableLobBuffer = null;
        }
    }

    @Override
    public void setUpIteration() {
        // Nothing to do.
    }

    @Override
    public void tearDownIteration() {
        // Nothing to do.
    }

    private void consumeCurrentValue(IonReader reader, boolean isInStruct) {
        if (isInStruct) {
            if (options.useSymbolTokens) {
                reader.getFieldNameSymbol();
            } else {
                reader.getFieldName();
            }
        }
        if (options.useSymbolTokens) {
            reader.getTypeAnnotationSymbols();
        } else {
            Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
            while (annotationsIterator.hasNext()) {
                annotationsIterator.next();
            }
        }
        IonType type = reader.getType();
        if (!reader.isNullValue()) {
            switch (type) {
                case BOOL:
                    reader.booleanValue();
                    break;
                case INT:
                    switch (reader.getIntegerSize()) {
                        case INT:
                            reader.intValue();
                            break;
                        case LONG:
                            reader.longValue();
                            break;
                        case BIG_INTEGER:
                            reader.bigIntegerValue();
                            break;
                    }
                    break;
                case FLOAT:
                    reader.doubleValue();
                    break;
                case DECIMAL:
                    if (options.ionUseBigDecimals) {
                        reader.bigDecimalValue();
                    } else {
                        reader.decimalValue();
                    }
                    break;
                case TIMESTAMP:
                    reader.timestampValue();
                    break;
                case SYMBOL:
                    if (options.useSymbolTokens) {
                        reader.symbolValue();
                    } else {
                        reader.stringValue();
                    }
                    break;
                case STRING:
                    reader.stringValue();
                    break;
                case CLOB:
                case BLOB:
                    if (options.useLobChunks) {
                        int bytesRemaining = reader.byteSize();
                        while (bytesRemaining > 0) {
                            bytesRemaining -= reader.getBytes(
                                reusableLobBuffer,
                                0,
                                Math.min(bytesRemaining, reusableLobBuffer.length)
                            );
                        }
                    } else {
                        reader.newBytes();
                    }
                    break;
                case LIST:
                case SEXP:
                    reader.stepIn();
                    fullyTraverse(reader, false);
                    reader.stepOut();
                    break;
                case STRUCT:
                    reader.stepIn();
                    fullyTraverse(reader, true);
                    reader.stepOut();
                    break;
                default:
                    break;
            }
        }
    }

    private void fullyTraverse(IonReader reader, boolean isInStruct) {
        while (reader.next() != null) {
            consumeCurrentValue(reader, isInStruct);
        }
    }


    @Override
    void fullyTraverseFromBuffer() throws IOException {
        IonReader reader = readerBuilder.build(buffer);
        fullyTraverse(reader, false);
        reader.close();
    }

    @Override
    public void fullyTraverseFromFile() throws IOException {
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        fullyTraverse(reader, false);
        reader.close();
    }

    @Override
    void traverseFromBuffer(List<String> paths) throws IOException {
        IonReader reader = readerBuilder.build(buffer);
        pathExtractor.match(reader);
        reader.close();
    }

    @Override
    public void traverseFromFile(List<String> paths) throws IOException {
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        pathExtractor.match(reader);
        reader.close();
    }

    @Override
    public void fullyReadDomFromBuffer() throws IOException {
        IonReader reader = readerBuilder.build(buffer);
        ionSystem.newLoader().load(reader);
        reader.close();
    }

    @Override
    public void fullyReadDomFromFile() throws IOException {
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        ionSystem.newLoader().load(reader);
        reader.close();
    }
}
