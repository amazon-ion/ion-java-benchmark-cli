package com.amazon.ion.benchmark;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.impl.LookaheadIonReaderWrapper;
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

    private static final int DEFAULT_NON_BLOCKING_BUFFER_SIZE = 64 * 1024;
    private final IonReaderBuilder readerBuilder;
    private final PathExtractor<?> pathExtractor;
    private final IonSystem ionSystem;

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
        readerBuilder = IonUtilities.newReaderBuilderForBenchmark(options).
            withNonBlockingEnabled(options.readerType == IonReaderType.NON_BLOCKING);
        if (readerBuilder.isNonBlockingEnabled()) {
            // TODO configurable initial buffer size for non-blocking to take precedence over the auto-tuned value.
            long inputSize = inputPath.toFile().length();
            if (inputSize < DEFAULT_NON_BLOCKING_BUFFER_SIZE){
                readerBuilder.withNonBlockingConfiguration(
                    new LookaheadIonReaderWrapper.Builder().withInitialBufferSize(nextPowerOfTwo((int) inputSize))
                );
            }
        }
        if (options.paths != null) {
            PathExtractorBuilder<?> pathExtractorBuilder = PathExtractorBuilder.standard();
            for (String path : options.paths) {
                pathExtractorBuilder.withSearchPath(path, this::pathExtractorCallback);
            }
            pathExtractor = pathExtractorBuilder.build();
        } else {
            pathExtractor = null;
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
        switch (reader.getType()) {
            case NULL:
                // In this case, you'd typically just return the type, which we've already retrieved above.
                break;
            case BOOL:
                reader.booleanValue();
                break;
            case INT:
                reader.longValue();
                break;
            case FLOAT:
                reader.doubleValue();
                break;
            case DECIMAL:
                reader.decimalValue();
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
                reader.newBytes();
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
