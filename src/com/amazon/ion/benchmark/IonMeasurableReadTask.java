package com.amazon.ion.benchmark;

import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.impl.LookaheadIonReaderWrapper;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;

/**
 * A MeasurableReadTask for reading data in the Ion format (either text or binary).
 */
class IonMeasurableReadTask extends MeasurableReadTask {

    private static final int DEFAULT_NON_BLOCKING_BUFFER_SIZE = 64 * 1024;
    private final IonReaderBuilder readerBuilder;
    private IonReader reader;
    private IonLoader loader;

    /**
     * Returns the next power of two greater than or equal to the given value.
     * @param value the start value.
     * @return the next power of two.
     */
    private static int nextPowerOfTwo(int value) {
        return (int) Math.pow(2, Math.ceil(Math.log10(value) / Math.log10(2)));
    }

    /**
     * @param inputPath the Ion data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    IonMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        readerBuilder = IonUtilities.newReaderBuilder(options).
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
    }

    // TODO make it configurable to perform reader/loader setup/teardown within the timed block.
    @Override
    public void setUpIteration() throws IOException {
        if (options.api == IonAPI.STREAMING) {
            if (buffer != null) {
                reader = readerBuilder.build(buffer);
            } else {
                reader = readerBuilder.build(options.newInputStream(inputPath.toFile()));
            }
            if (options.paths != null) {
                // TODO create path extractor.
            }
            loader = null;
        } else {
            loader = ION_SYSTEM.getLoader();
            reader = null;
        }
    }

    @Override
    public void tearDownIteration() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private static void fullyTraverse(IonReader reader, boolean isInStruct) {
        while (reader.next() != null) {
            if (isInStruct) {
                reader.getFieldName();
            }
            Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
            while (annotationsIterator.hasNext()) {
                annotationsIterator.next();
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
                    // TODO add option for bigDecimalValue
                    reader.decimalValue();
                    break;
                case TIMESTAMP:
                    reader.timestampValue();
                    break;
                case SYMBOL:
                    // TODO add option for symbolValue
                    reader.stringValue();
                    break;
                case STRING:
                    reader.stringValue();
                    break;
                case CLOB:
                case BLOB:
                    // TODO add option for getBytes
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
    }

    @Override
    public void fullyTraverse() {
        fullyTraverse(reader, false);
    }

    @Override
    public void traverse(List<String> paths) {
        throw new IllegalStateException("Not yet implemented.");
    }

    @Override
    public void fullyReadDomFromBuffer() throws IOException {
        loader.load(buffer);
    }

    @Override
    public void fullyReadDomFromFile() throws IOException {
        loader.load(inputPath.toFile());
    }
}
