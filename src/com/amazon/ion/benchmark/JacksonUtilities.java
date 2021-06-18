package com.amazon.ion.benchmark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Utility class for Jackson APIs.
 */
class JacksonUtilities {

    private JacksonUtilities() {
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
    static JsonGeneratorSupplier newJsonGeneratorSupplier(OptionsCombinationBase options) {
        JsonFactory factory = newJsonFactoryForInput(options);
        if (options.api == API.DOM) {
            factory.setCodec(newJsonObjectMapper(factory, options));
        }
        return factory::createGenerator;
    }

    /**
     * Creates a new ObjectMapper instance using the given JsonFactory and options.
     * @param jsonFactory the JsonFactory from which to construct the ObjectMapper.
     * @param options the options to use when configuring the ObjectMapper.
     * @return a new instance.
     */
    static ObjectMapper newJsonObjectMapper(JsonFactory jsonFactory, OptionsCombinationBase options) {
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
     * Supplies CBORGenerator instances for the given options.
     */
    @FunctionalInterface
    interface CborGeneratorSupplier {
        CBORGenerator get(OutputStream outputStream) throws IOException;
    }

    /**
     * Creates a new supplier of CBORGenerator instances.
     * @param options the options to use when creating generators.
     * @return a new instance.
     */
    static CborGeneratorSupplier newCborGeneratorSupplier(OptionsCombinationBase options) {
        CBORFactory factory = newCborFactoryForInput(options);
        if (options.api == API.DOM) {
            factory.setCodec(newCborObjectMapper(factory, options));
        }
        return factory::createGenerator;
    }

    /**
     * Creates a new ObjectMapper instance using the given CBORFactory and options.
     * @param cborFactory the CBORFactory from which to construct the ObjectMapper.
     * @param options the options to use when configuring the ObjectMapper.
     * @return a new instance.
     */
    static CBORMapper newCborObjectMapper(CBORFactory cborFactory, OptionsCombinationBase options) {
        return new CBORMapper(cborFactory);
    }

    /**
     * Creates a new CBORFactory instance using the given options.
     * @param options the options to use when configuring the CBORFactory.
     * @return a new instance.
     */
    static CBORFactory newCborFactoryForInput(OptionsCombinationBase options) {
        // Note: if any Jackson CBOR-specific factory options are added, they will be configured here.
        return CBORFactory.builder()
            .configure(CBORGenerator.Feature.WRITE_TYPE_HEADER, true)
            // TODO make CBORGenerator.Feature.WRITE_MINIMAL_INTS configurable
            .build();
    }

    /**
     * Rewrites the given parser's data using the given generator.
     * @param parser parser for the input data.
     * @param generator generator for the output data.
     * @param parserMapper mapper for the parser.
     * @param generatorMapper mapper for the generator.
     * @param options options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    private static void rewriteWithJackson(
        JsonParser parser,
        JsonGenerator generator,
        ObjectMapper parserMapper,
        ObjectMapper generatorMapper,
        OptionsCombinationBase options
    ) throws IOException {
        int i = 0;
        boolean isUnlimited = options.limit == Integer.MAX_VALUE;
        parser.setCodec(parserMapper);
        generator.setCodec(generatorMapper);
        while (isUnlimited || i < options.limit) {
            if (parser.nextValue() == null) {
                break;
            }
            generatorMapper.writeTree(generator, parser.readValueAsTree());
            i++;
        }
    }

    /**
     * Rewrite the given JSON file using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteJsonFile(Path input, Path output, OptionsCombinationBase options) throws IOException {
        JsonFactory jsonFactory = newJsonFactoryForInput(options);
        ObjectMapper mapper = newJsonObjectMapper(jsonFactory, options);
        try (
            InputStream inputStream = options.newInputStream(input.toFile());
            JsonParser parser = jsonFactory.createParser(inputStream);
            OutputStream outputStream = options.newOutputStream(output.toFile());
            JsonGenerator generator = jsonFactory.createGenerator(outputStream)
                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, options.jsonUseBigDecimals)
        ) {
            rewriteWithJackson(parser, generator, mapper, mapper, options);
        }
    }

    /**
     * Rewrite the given CBOR file using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteCborFile(Path input, Path output, OptionsCombinationBase options) throws IOException {
        CBORFactory cborFactory = newCborFactoryForInput(options);
        CBORMapper mapper = newCborObjectMapper(cborFactory, options);
        try (
            InputStream inputStream = options.newInputStream(input.toFile());
            CBORParser parser = cborFactory.createParser(inputStream);
            OutputStream outputStream = options.newOutputStream(output.toFile());
            CBORGenerator generator = cborFactory.createGenerator(outputStream)
        ) {
            rewriteWithJackson(parser, generator, mapper, mapper, options);
        }
    }

    /**
     * Convert the given JSON file to CBOR using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteJsonToCbor(Path input, Path output, OptionsCombinationBase options) throws IOException {
        JsonFactory jsonFactory = newJsonFactoryForInput(options);
        CBORFactory cborFactory = newCborFactoryForInput(options);
        ObjectMapper jsonMapper = newJsonObjectMapper(jsonFactory, options);
        CBORMapper cborMapper = newCborObjectMapper(cborFactory, options);
        try (
            InputStream inputStream = options.newInputStream(input.toFile());
            JsonParser parser = jsonFactory.createParser(inputStream);
            OutputStream outputStream = options.newOutputStream(output.toFile());
            CBORGenerator generator = cborFactory.createGenerator(outputStream)
        ) {
            rewriteWithJackson(parser, generator, jsonMapper, cborMapper, options);
        }
    }

    /**
     * Convert the given CBOR file to JSON using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteCborToJson(Path input, Path output, OptionsCombinationBase options) throws IOException {
        JsonFactory jsonFactory = newJsonFactoryForInput(options);
        CBORFactory cborFactory = newCborFactoryForInput(options);
        ObjectMapper jsonMapper = newJsonObjectMapper(jsonFactory, options);
        CBORMapper cborMapper = newCborObjectMapper(cborFactory, options);
        try (
            InputStream inputStream = options.newInputStream(input.toFile());
            CBORParser parser = cborFactory.createParser(inputStream);
            OutputStream outputStream = options.newOutputStream(output.toFile());
            JsonGenerator generator = jsonFactory.createGenerator(outputStream)
                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, options.jsonUseBigDecimals)
        ) {
            rewriteWithJackson(parser, generator, cborMapper, jsonMapper, options);
        }
    }
}
