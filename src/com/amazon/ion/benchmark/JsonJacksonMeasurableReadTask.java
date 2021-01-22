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
            parser.getCurrentName();
        }
        switch(parser.getCurrentToken()) {
            case VALUE_TRUE:
            case VALUE_FALSE:
                parser.getBooleanValue();
                break;
            case VALUE_NUMBER_INT:
                switch (parser.getNumberType()) {
                    case INT:
                        parser.getIntValue();
                        break;
                    case LONG:
                        parser.getLongValue();
                        break;
                    case BIG_INTEGER:
                        parser.getBigIntegerValue();
                        break;
                }
                break;
            case VALUE_NUMBER_FLOAT:
                if (options.jsonUseBigDecimals) {
                    parser.getDecimalValue();
                } else {
                    parser.getDoubleValue();
                }
                break;
            case VALUE_STRING:
                parser.getValueAsString();
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
    void fullyTraverseFromBuffer() throws IOException {
        JsonParser parser = jsonFactory.createParser(buffer);
        fullyTraverse(parser, false);
        parser.close();
    }

    @Override
    void fullyTraverseFromFile() throws IOException {
        JsonParser parser = jsonFactory.createParser(options.newInputStream(inputFile));
        fullyTraverse(parser, false);
        parser.close();
    }

    @Override
    void traverseFromBuffer(List<String> paths) throws IOException {
        // If Jackson provides a path extraction API for sparse reads, use that.
        fullyTraverseFromBuffer();
    }

    @Override
    void traverseFromFile(List<String> paths) throws IOException {
        // If Jackson provides a path extraction API for sparse reads, use that.
        fullyTraverseFromFile();
    }

    @Override
    void fullyReadDomFromBuffer() throws IOException {
        ObjectMapper mapper = JsonJacksonUtilities.newObjectMapper(jsonFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(buffer).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    @Override
    void fullyReadDomFromFile() throws IOException {
        ObjectMapper mapper = JsonJacksonUtilities.newObjectMapper(jsonFactory, options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(options.newInputStream(inputFile)).readValuesAs(JsonNode.class);
        while (iterator.hasNext()) {
            iterator.next();
        }
    }
}
