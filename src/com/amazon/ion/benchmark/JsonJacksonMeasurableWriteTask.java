package com.amazon.ion.benchmark;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * A MeasurableWriteTask for writing data in the JSON format using the Jackson library.
 */
class JsonJacksonMeasurableWriteTask extends MeasurableWriteTask<JsonGenerator> {

    private final JsonJacksonUtilities.JsonGeneratorSupplier generatorSupplier;

    /**
     * @param inputPath path to the data to re-write.
     * @param options options to use when writing.
     * @throws IOException if thrown when handling the options.
     */
    JsonJacksonMeasurableWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
        super(inputPath, options);
        generatorSupplier = JsonJacksonUtilities.newGeneratorSupplier(options);
    }

    /**
     * Generate WriteInstructions by fully traversing the input data.
     * @param parser JsonParser over the input data.
     * @param instructionsSink sink for the generated WriteInstructions.
     */
    private void fullyTraverse(
        JsonParser parser,
        Consumer<WriteInstruction<JsonGenerator>> instructionsSink,
        boolean isTopLevel
    ) throws IOException {
        int numberOfTopLevelValues = 0;
        while (parser.nextValue() != null) {
            JsonToken token = parser.getCurrentToken();
            if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                break;
            }
            String fieldName = parser.getCurrentName();
            if (fieldName != null) {
                instructionsSink.accept(generator -> generator.writeFieldName(fieldName));
            }
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    instructionsSink.accept(JsonGenerator::writeNull);
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    boolean booleanValue = parser.getBooleanValue();
                    instructionsSink.accept(generator -> generator.writeBoolean(booleanValue));
                    break;
                case VALUE_NUMBER_INT:
                    switch (parser.getNumberType()) {
                        case INT:
                            int intValue = parser.getIntValue();
                            instructionsSink.accept(generator -> generator.writeNumber(intValue));
                            break;
                        case LONG:
                            long longValue = parser.getLongValue();
                            instructionsSink.accept(generator -> generator.writeNumber(longValue));
                            break;
                        case BIG_INTEGER:
                            BigInteger bigIntegerValue = parser.getBigIntegerValue();
                            instructionsSink.accept(generator -> generator.writeNumber(bigIntegerValue));
                            break;
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    if (options.jsonUseBigDecimals) {
                        BigDecimal bigDecimalValue = parser.getDecimalValue();
                        instructionsSink.accept(generator -> generator.writeNumber(bigDecimalValue));
                    } else {
                        double doubleValue = parser.getDoubleValue();
                        instructionsSink.accept(generator -> generator.writeNumber(doubleValue));
                    }
                    break;
                case VALUE_STRING:
                    String stringValue = parser.getValueAsString();
                    instructionsSink.accept(generator -> generator.writeString(stringValue));
                    break;
                case START_ARRAY:
                    instructionsSink.accept(JsonGenerator::writeStartArray);
                    fullyTraverse(parser, instructionsSink, false);
                    instructionsSink.accept(JsonGenerator::writeEndArray);
                    break;
                case START_OBJECT:
                    instructionsSink.accept(JsonGenerator::writeStartObject);
                    fullyTraverse(parser, instructionsSink, false);
                    instructionsSink.accept(JsonGenerator::writeEndObject);
                    break;
                default:
                    throw new IllegalStateException("Found an unexpected token: " + parser.getCurrentToken());
            }
            if (isTopLevel) {
                numberOfTopLevelValues++;
                if (options.limit != Integer.MAX_VALUE && numberOfTopLevelValues >= options.limit) {
                    break;
                }
            }
        }
    }

    @Override
    void generateWriteInstructionsStreaming(Consumer<WriteInstruction<JsonGenerator>> instructionsSink) throws IOException {
        try (JsonParser parser = JsonJacksonUtilities.newJsonFactoryForInput(options).createParser(options.newInputStream(inputFile))) {
            fullyTraverse(parser, instructionsSink, true);
        }
    }

    @Override
    void generateWriteInstructionsDom(Consumer<WriteInstruction<JsonGenerator>> instructionsSink) throws IOException {
        ObjectMapper mapper = JsonJacksonUtilities.newObjectMapper(JsonJacksonUtilities.newJsonFactoryForInput(options), options);
        Iterator<JsonNode> iterator = mapper.reader().createParser(options.newInputStream(inputFile)).readValuesAs(JsonNode.class);
        int numberOfValues = 0;
        while (iterator.hasNext()) {
            JsonNode value = iterator.next();
            instructionsSink.accept(generator -> generator.writeTree(value));
            numberOfValues++;
            if (options.limit != Integer.MAX_VALUE && numberOfValues >= options.limit) {
                break;
            }
        }
    }

    @Override
    JsonGenerator newWriter(OutputStream outputStream) throws IOException {
        return generatorSupplier.get(outputStream);
    }

    @Override
    void closeWriter(JsonGenerator generator) throws IOException {
        generator.close();
    }
}
