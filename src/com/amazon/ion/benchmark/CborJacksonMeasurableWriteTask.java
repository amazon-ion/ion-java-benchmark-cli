package com.amazon.ion.benchmark;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * A MeasurableWriteTask for writing data in the CBOR format using the Jackson library.
 */
public class CborJacksonMeasurableWriteTask extends MeasurableWriteTask<CBORGenerator> {

    private final JacksonUtilities.CborGeneratorSupplier generatorSupplier;

    /**
     * @param inputPath path to the data to re-write.
     * @param options options to use when writing.
     * @throws IOException if thrown when handling the options.
     */
    CborJacksonMeasurableWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
        super(inputPath, options);
        generatorSupplier = JacksonUtilities.newCborGeneratorSupplier(options);
    }

    /**
     * Generate WriteInstructions by fully traversing the input data.
     * @param parser JsonParser over the input data.
     * @param instructionsSink sink for the generated WriteInstructions.
     */
    private void fullyTraverse(
        CBORParser parser,
        Consumer<WriteInstruction<CBORGenerator>> instructionsSink,
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
                    instructionsSink.accept(CBORGenerator::writeNull);
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
                    if (parser.getNumberType() == JsonParser.NumberType.BIG_DECIMAL) {
                        BigDecimal bigDecimalValue = parser.getDecimalValue();
                        instructionsSink.accept(generator -> generator.writeNumber(bigDecimalValue));
                    } else {
                        double doubleValue = parser.getDoubleValue();
                        instructionsSink.accept(generator -> generator.writeNumber(doubleValue));
                    }
                    break;
                case VALUE_STRING:
                    String stringValue = parser.getValueAsString();
                    if (parser.getCurrentTag() == 0) {
                        instructionsSink.accept(generator -> generator.writeTag(0));
                    }
                    instructionsSink.accept(generator -> generator.writeString(stringValue));
                    break;
                case VALUE_EMBEDDED_OBJECT:
                    byte[] byteValue = parser.getBinaryValue();
                    instructionsSink.accept(generator -> generator.writeBinary(byteValue));
                    break;
                case START_ARRAY:
                    instructionsSink.accept(CBORGenerator::writeStartArray);
                    fullyTraverse(parser, instructionsSink, false);
                    instructionsSink.accept(CBORGenerator::writeEndArray);
                    break;
                case START_OBJECT:
                    instructionsSink.accept(CBORGenerator::writeStartObject);
                    fullyTraverse(parser, instructionsSink, false);
                    instructionsSink.accept(CBORGenerator::writeEndObject);
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
    void generateWriteInstructionsStreaming(Consumer<WriteInstruction<CBORGenerator>> instructionsSink) throws IOException {
        try (CBORParser parser = JacksonUtilities.newCborFactoryForInput(options).createParser(options.newInputStream(inputFile))) {
            fullyTraverse(parser, instructionsSink, true);
        }
    }

    @Override
    void generateWriteInstructionsDom(Consumer<WriteInstruction<CBORGenerator>> instructionsSink) throws IOException {
        CBORMapper mapper = JacksonUtilities.newCborObjectMapper(JacksonUtilities.newCborFactoryForInput(options), options);
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
    CBORGenerator newWriter(OutputStream outputStream) throws IOException {
        return generatorSupplier.get(outputStream);
    }

    @Override
    void closeWriter(CBORGenerator generator) throws IOException {
        generator.close();
    }
}
