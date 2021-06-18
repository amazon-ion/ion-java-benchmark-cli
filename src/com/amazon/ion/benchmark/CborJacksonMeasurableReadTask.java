package com.amazon.ion.benchmark;

import com.amazon.ion.Timestamp;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * A MeasurableReadTask for reading data in the CBOR format using the Jackson library.
 */
public class CborJacksonMeasurableReadTask extends MeasurableReadTask {

    private final CBORFactory cborFactory;
    private SideEffectConsumer sideEffectConsumer = null;

    /**
     * @param inputPath the JSON data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    CborJacksonMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        cborFactory = JacksonUtilities.newCborFactoryForInput(options);
        if (options.paths != null) {
            System.out.println(
                "WARNING: Jackson does not provide a path-based value extraction API for efficient sparse reads. "
                    + "Falling back to full traversal."
            );
        }
    }

    private boolean consumeCurrentValue(CBORParser parser, boolean isInStruct) throws IOException {
        if (isInStruct) {
            sideEffectConsumer.consume(parser.getCurrentName());
        }
        switch(parser.getCurrentToken()) {
            case VALUE_NULL:
                // There is nothing to do for null.
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
                sideEffectConsumer.consume(parser.getBooleanValue());
                break;
            case VALUE_NUMBER_INT:
                switch (parser.getNumberType()) {
                    case INT:
                        sideEffectConsumer.consume(parser.getIntValue());
                        break;
                    case LONG:
                        sideEffectConsumer.consume(parser.getLongValue());
                        break;
                    case BIG_INTEGER:
                        sideEffectConsumer.consume(parser.getBigIntegerValue());
                        break;
                }
                break;
            case VALUE_NUMBER_FLOAT:
                if (parser.getNumberType() == JsonParser.NumberType.BIG_DECIMAL) {
                    sideEffectConsumer.consume(parser.getDecimalValue());
                } else {
                    sideEffectConsumer.consume(parser.getDoubleValue());
                }
                break;
            case VALUE_STRING:
                if (parser.getCurrentTag() == 0) {
                    sideEffectConsumer.consume(Timestamp.valueOf(parser.getValueAsString()));
                } else {
                    sideEffectConsumer.consume(parser.getValueAsString());
                }
                break;
            case VALUE_EMBEDDED_OBJECT:
                sideEffectConsumer.consume(parser.getBinaryValue());
                break;
            case START_ARRAY:
                fullyTraverse(parser, false);
                break;
            case START_OBJECT:
                fullyTraverse(parser, true);
                break;
            case END_ARRAY:
            case END_OBJECT:
                return true;
            default:
                break;
        }
        return false;
    }

    private void fullyTraverse(CBORParser parser, boolean isInStruct) throws IOException {
        while (parser.nextValue() != null) {
            if (consumeCurrentValue(parser, isInStruct)) {
                break;
            }
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

    @Override
    void fullyTraverseFromBuffer(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        CBORParser parser = cborFactory.createParser(buffer);
        fullyTraverse(parser, false);
        parser.close();
    }

    @Override
    void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        CBORParser parser = cborFactory.createParser(options.newInputStream(inputFile));
        fullyTraverse(parser, false);
        parser.close();
    }

    @Override
    void traverseFromBuffer(List<String> paths, SideEffectConsumer consumer) throws IOException {
        // If Jackson provides a path extraction API for sparse reads, use that.
        fullyTraverseFromBuffer(consumer);
    }

    @Override
    void traverseFromFile(List<String> paths, SideEffectConsumer consumer) throws IOException {
        // If Jackson provides a path extraction API for sparse reads, use that.
        fullyTraverseFromFile(consumer);
    }

    @Override
    void fullyReadDomFromBuffer(SideEffectConsumer consumer) throws IOException {
        CBORMapper mapper = JacksonUtilities.newCborObjectMapper(cborFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(buffer).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            consumer.consume(iterator.next());
        }
    }

    @Override
    void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException {
        CBORMapper mapper = JacksonUtilities.newCborObjectMapper(cborFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(options.newInputStream(inputFile)).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            consumer.consume(iterator.next());
        }
    }
}
