package com.amazon.ion.benchmark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Utility class for Jackson JSON-related functions.
 */
class JsonJacksonUtilities {

    private JsonJacksonUtilities() {
        // Do not instantiate.
    }

    /**
     * Supplies JsonGenerator instances for the given options.
     */
    @FunctionalInterface
    interface JsonGeneratorSupplier {
        JsonGenerator get(OutputStream outputStream) throws IOException;
    }

    /**
     * Creates a new supplier of JsonGenerator instances.
     * @param options the options to use when creating generators.
     * @return a new instance.
     */
    static JsonGeneratorSupplier newGeneratorSupplier(OptionsCombinationBase options) {
        JsonFactory factory = newJsonFactoryForInput(options);
        if (options.api == API.DOM) {
            factory.setCodec(newObjectMapper(factory, options));
        }
        return factory::createGenerator;
    }

    /**
     * Creates a new ObjectMapper instance using the given JsonFactory and options.
     * @param jsonFactory the JsonFactory from which to construct the ObjectMapper.
     * @param options the options to use when configuring the ObjectMapper.
     * @return a new instance.
     */
    static ObjectMapper newObjectMapper(JsonFactory jsonFactory, OptionsCombinationBase options) {
        ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
        if (options.jsonUseBigDecimals) {
            objectMapper = objectMapper
                .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        }
        return objectMapper;
    }

    /**
     * Creates a new JsonFactory instance using the given options.
     * @param options the options to use when configuring the JsonFactory.
     * @return a new instance.
     */
    static JsonFactory newJsonFactoryForInput(OptionsCombinationBase options) {
        // Note: if any Jackson-specific factory options are added, they will be configured here.
        return new JsonFactoryBuilder().build();
    }

    /**
     * Rewrite the given JSON file using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteJsonFile(Path input, Path output, OptionsCombinationBase options) throws IOException {
        int i = 0;
        boolean isUnlimited = options.limit == Integer.MAX_VALUE;
        JsonFactory jsonFactory = newJsonFactoryForInput(options);
        ObjectMapper mapper = newObjectMapper(jsonFactory, options);
        try (
            InputStream inputStream = options.newInputStream(input.toFile());
            JsonParser parser = jsonFactory.createParser(inputStream);
            OutputStream outputStream = options.newOutputStream(output.toFile());
            JsonGenerator generator = jsonFactory.createGenerator(outputStream)
                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, options.jsonUseBigDecimals)
        ) {
            parser.setCodec(mapper);
            generator.setCodec(mapper);
            while (isUnlimited || i < options.limit) {
                if (parser.nextValue() == null) {
                    break;
                }
                mapper.writeTree(generator, parser.readValueAsTree());
                i++;
            }
        }
    }
}
