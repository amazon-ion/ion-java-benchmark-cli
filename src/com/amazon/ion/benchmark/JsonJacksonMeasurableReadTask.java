package com.amazon.ion.benchmark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * A MeasurableReadTask for reading data in the JSON format using the Jackson library.
 */
public class JsonJacksonMeasurableReadTask extends MeasurableReadTask {

    private final JsonFactory jsonFactory;
    private SideEffectConsumer sideEffectConsumer = null;

    /**
     * @param inputPath the JSON data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    JsonJacksonMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        jsonFactory = JsonJacksonUtilities.newJsonFactoryForInput(options);
        if (options.paths != null) {
            System.out.println(
                "WARNING: Jackson does not provide a path-based value extraction API for efficient sparse reads. "
                    + "Falling back to full traversal."
            );
        }
    }

    private boolean consumeCurrentValue(JsonParser parser, boolean isInStruct) throws IOException {
        if (isInStruct) {
            sideEffectConsumer.consume(parser.getCurrentName());
        }
        switch(parser.getCurrentToken()) {
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
                if (options.jsonUseBigDecimals) {
                    sideEffectConsumer.consume(parser.getDecimalValue());
                } else {
                    sideEffectConsumer.consume(parser.getDoubleValue());
                }
                break;
            case VALUE_STRING:
                sideEffectConsumer.consume(parser.getValueAsString());
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
                // Note: includes null values.
                break;
        }
        return false;
    }

    private void fullyTraverse(JsonParser parser, boolean isInStruct) throws IOException {
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
        JsonParser parser = jsonFactory.createParser(buffer);
        fullyTraverse(parser, false);
        parser.close();
    }

    @Override
    void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        JsonParser parser = jsonFactory.createParser(options.newInputStream(inputFile));
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
        ObjectMapper mapper = JsonJacksonUtilities.newObjectMapper(jsonFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(buffer).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            consumer.consume(iterator.next());
        }
    }

    @Override
    void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException {
        ObjectMapper mapper = JsonJacksonUtilities.newObjectMapper(jsonFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(options.newInputStream(inputFile)).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            consumer.consume(iterator.next());
        }
    }
}
