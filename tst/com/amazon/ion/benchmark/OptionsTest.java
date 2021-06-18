package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.Equivalence;
import com.amazon.ion.util.IonStreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NumericNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OptionsTest {

    /**
     * Gets the Path to a file in the same directory as OptionsTest.
     * @param simpleName the simple name of the file.
     * @return a new Path.
     */
    private static Path fileInTestDirectory(String simpleName) {
        return Paths.get("tst", "com", "amazon", "ion", "benchmark", simpleName);
    }

    /**
     * Asserts Ion data model equality using the given options.
     * @param expected the expected Ion data.
     * @param actual the actual Ion data.
     * @param options the options, which include the shared symbol tables to use (if any) and the maximum number of
     *                values to compare.
     * @throws IOException if thrown while trying to read a file.
     */
    private static void assertIonEquals(File expected, byte[] actual, OptionsCombinationBase options) throws IOException {
        IonSystem systemForInput = IonUtilities.ionSystemForInput(options);
        IonSystem systemForBenchmark = IonUtilities.ionSystemForBenchmark(options);
        IonDatagram expectedDatagram = systemForInput.getLoader().load(expected);
        IonDatagram actualDatagram = systemForBenchmark.getLoader().load(actual);
        if (options.limit == Integer.MAX_VALUE) {
            assertTrue(Equivalence.ionEquals(expectedDatagram, actualDatagram));
        } else {
            assertEquals(options.limit, actualDatagram.size());
            for (int i = 0; i < options.limit; i++) {
                Equivalence.ionEquals(expectedDatagram.get(i), actualDatagram.get(i));
            }
        }
    }

    /**
     * Basic comparator used to compare Jackson JsonNodes, which are used to represent both JSON and CBOR data.
     */
    private static final Comparator<JsonNode> BASIC_JSON_NODE_COMPARATOR = (o1, o2) -> {
        if (o1.equals(o2)) {
            return 0;
        }
        return 1;
    };

    /**
     * The JSON data model only has a single number type, but the default implementation of JsonNode.equals requires
     * values to have been read into the same Java type even if their values are equivalent. This causes problems
     * with certain Ion translations (e.g. decimal negative zero to float zero), so we will use a custom comparator
     * that up-converts all numbers to BigDecimal (thereby preserving precision) before comparison.
     */
    private static final Comparator<JsonNode> UNIFIED_NUMBER_TYPE_JSON_NODE_COMPARATOR = (o1, o2) -> {
        if (o1.equals(o2)) {
            return 0;
        }
        if ((o1 instanceof NumericNode) && (o2 instanceof NumericNode)) {
            return o1.decimalValue().compareTo(o2.decimalValue());
        }
        return 1;
    };

    /**
     * Asserts Jackson JsonNode equality using the given options.
     * @param expected the expected JSON data.
     * @param actual the actual JSON data.
     * @param options the options, which include the maximum number of values to compare.
     * @param mapper the ObjectMapper to use to read the data into JsonNodes.
     * @param comparator the JsonNode comparator.
     * @throws IOException if thrown while trying to read a file.
     */
    private static void assertEqualsWithJackson(
        File expected,
        byte[] actual,
        OptionsCombinationBase options,
        ObjectMapper mapper,
        Comparator<JsonNode> comparator
    ) throws IOException {
        List<JsonNode> expectedValues = new ArrayList<>();
        mapper.reader()
            .createParser(options.newInputStream(expected))
            .readValuesAs(JsonNode.class)
            .forEachRemaining(expectedValues::add);
        List<JsonNode> actualValues = new ArrayList<>();
        mapper.reader()
            .createParser(actual)
            .readValuesAs(JsonNode.class)
            .forEachRemaining(actualValues::add);
        if (options.limit == Integer.MAX_VALUE) {
            assertEquals(expectedValues.size(), actualValues.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                assertTrue(expectedValues.get(i).equals(comparator, actualValues.get(i)));
            }
        } else {
            assertEquals(options.limit, actualValues.size());
            for (int i = 0; i < options.limit; i++) {
                assertTrue(expectedValues.get(i).equals(comparator, actualValues.get(i)));
            }
        }
    }

    /**
     * Asserts JSON data model equality using the given options.
     * @param expected the expected JSON data.
     * @param actual the actual JSON data.
     * @param options the options, which include the maximum number of values to compare.
     * @throws IOException if thrown while trying to read a file.
     */
    private static void assertJsonEquals(File expected, byte[] actual, OptionsCombinationBase options) throws IOException {
        ObjectMapper mapper = JacksonUtilities.newJsonObjectMapper(
            JacksonUtilities.newJsonFactoryForInput(options),
            options
        );
        assertEqualsWithJackson(expected, actual, options, mapper, UNIFIED_NUMBER_TYPE_JSON_NODE_COMPARATOR);
    }

    /**
     * Asserts CBOR data model equality using the given options.
     * @param expected the expected CBOR data.
     * @param actual the actual CBOR data.
     * @param options the options, which include the maximum number of values to compare.
     * @throws IOException if thrown while trying to read a file.
     */
    private static void assertCborEquals(File expected, byte[] actual, OptionsCombinationBase options) throws IOException {
        ObjectMapper mapper = JacksonUtilities.newCborObjectMapper(
            JacksonUtilities.newCborFactoryForInput(options),
            options
        );
        assertEqualsWithJackson(expected, actual, options, mapper, BASIC_JSON_NODE_COMPARATOR);
    }

    /**
     * Asserts that the expected data is equivalent to the actual data, using the given Format's definition of
     * equivalence.
     * @param format the format supplying the equivalence definition for the data.
     * @param expected the expected data.
     * @param actual the actual data.
     * @param options the options to use during comparison.
     * @throws IOException if thrown while trying to read a file.
     */
    private static void assertDataEquals(Format format, File expected, byte[] actual, OptionsCombinationBase options) throws IOException {
        switch (format) {
            case ION_BINARY:
            case ION_TEXT:
                assertIonEquals(expected, actual, options);
                break;
            case JSON:
                assertJsonEquals(expected, actual, options);
                break;
            case CBOR:
                assertCborEquals(expected, actual, options);
                break;
            default:
                throw new IllegalStateException("Equality function must be added for " + format);
        }
    }

    /**
     * Asserts that the given data is in the expected format.
     * @param buffer buffer containing the data to test.
     * @param expectedFormat the format to assert.
     */
    private static void assertFormat(byte[] buffer, Format expectedFormat) {
        assertEquals(expectedFormat == Format.ION_BINARY, IonStreamUtils.isIonBinary(buffer));
        assertEquals(expectedFormat == Format.CBOR, CborUtilities.isCbor(buffer));
    }

    /**
     * Base class for expected read/write options combinations.
     * @param <T> the concrete implementation of this class.
     * @param <U> the type of {@link OptionsCombinationBase} to test against.
     */
    private abstract static class ExpectedOptionsCombinationBase<T extends ExpectedOptionsCombinationBase, U extends OptionsCombinationBase> {
        Integer preallocation = null;
        Integer flushPeriod = null;
        Format format = Format.ION_BINARY;
        API api = API.STREAMING;
        Integer ioBufferSize = null;
        IoType ioType = IoType.FILE;
        String importsForInputFile = null;
        String importsForBenchmarkFile = null;
        int limit = Integer.MAX_VALUE;
        boolean useSymbolTokens = false;
        Integer floatWidth = null;
        boolean jsonUseBigDecimals = true;

        final T preallocation(Integer preallocation) {
            this.preallocation = preallocation;
            return (T) this;
        }

        final T flushPeriod(Integer flushPeriod) {
            this.flushPeriod = flushPeriod;
            return (T) this;
        }

        final T format(Format format) {
            this.format = format;
            return (T) this;
        }

        final T api(API api) {
            this.api = api;
            return (T) this;
        }

        final T ioType(IoType ioType) {
            this.ioType = ioType;
            return (T) this;
        }

        final T ioBufferSize(Integer bufferSize) {
            this.ioBufferSize = bufferSize;
            return (T) this;
        }

        final T importsForBenchmarkFile(String importsFile) {
            this.importsForBenchmarkFile = importsFile;
            return (T) this;
        }

        final T importsForInputFile(String importsFile) {
            this.importsForInputFile = importsFile;
            return (T) this;
        }

        final T limit(int limit) {
            this.limit = limit;
            return (T) this;
        }

        final T useSymbolTokens(boolean useSymbolTokens) {
            this.useSymbolTokens = useSymbolTokens;
            return (T) this;
        }

        final T floatWidth(Integer floatWidth) {
            this.floatWidth = floatWidth;
            return (T) this;
        }

        final T jsonUseBigDecimals(boolean jsonUseBigDecimals) {
            this.jsonUseBigDecimals = jsonUseBigDecimals;
            return (T) this;
        }

        void assertOptionsEqual(U that) {
            assertEquals(flushPeriod, that.flushPeriod);
            assertEquals(api, that.api);
            assertEquals(format, that.format);
            assertEquals(limit, that.limit);
            assertEquals(preallocation, that.preallocation);
            assertEquals(importsForInputFile, that.importsForInputFile);
            assertEquals(importsForBenchmarkFile, that.importsForBenchmarkFile);
            assertEquals(ioType, that.ioType);
            assertEquals(ioBufferSize, that.ioBufferSize);
            assertEquals(floatWidth, that.floatWidth);
            assertEquals(jsonUseBigDecimals, that.jsonUseBigDecimals);
        }
    }

    /**
     * Test class for asserting that {@link ReadOptionsCombination} instances match expected values.
     */
    private static class ExpectedReadOptionsCombination
        extends ExpectedOptionsCombinationBase<ExpectedReadOptionsCombination, ReadOptionsCombination> {

        List<String> paths = null;
        IonReaderType readerType = IonReaderType.INCREMENTAL;
        boolean useLobChunks = false;
        boolean useBigDecimals = false;
        Integer initialBufferSize = null;

        static ExpectedReadOptionsCombination defaultOptions() {
            return new ExpectedReadOptionsCombination();
        }

        final ExpectedReadOptionsCombination paths(List<String> paths) {
            this.paths = paths;
            return this;
        }

        final ExpectedReadOptionsCombination readerType(IonReaderType readerType) {
            this.readerType = readerType;
            return this;
        }

        final ExpectedReadOptionsCombination useLobChunks(boolean useLobChunks) {
            this.useLobChunks = useLobChunks;
            return this;
        }

        final ExpectedReadOptionsCombination useBigDecimals(boolean useBigDecimals) {
            this.useBigDecimals = useBigDecimals;
            return this;
        }

        final ExpectedReadOptionsCombination initialBufferSize(Integer initialBufferSize) {
            this.initialBufferSize = initialBufferSize;
            return this;
        }

        @Override
        void assertOptionsEqual(ReadOptionsCombination that) {
            super.assertOptionsEqual(that);
            assertEquals(paths, that.paths);
            assertEquals(readerType, that.readerType);
            assertEquals(useLobChunks, that.useLobChunks);
            assertEquals(useBigDecimals, that.ionUseBigDecimals);
            assertEquals(initialBufferSize, that.initialBufferSize);
        }
    }

    /**
     * Test class for asserting that {@link WriteOptionsCombination} instances match expected values.
     */
    private static class ExpectedWriteOptionsCombination
        extends ExpectedOptionsCombinationBase<ExpectedWriteOptionsCombination, WriteOptionsCombination> {

        Integer ionWriterBlockSize = null;

        static ExpectedWriteOptionsCombination defaultOptions() {
            return new ExpectedWriteOptionsCombination();
        }

        final ExpectedWriteOptionsCombination ionWriterBlockSize(Integer ionWriterBlockSize) {
            this.ionWriterBlockSize = ionWriterBlockSize;
            return this;
        }

        @Override
        void assertOptionsEqual(WriteOptionsCombination that) {
            super.assertOptionsEqual(that);
            assertEquals(ionWriterBlockSize, that.ionWriterBlockSize);
        }
    }

    /**
     * Determines whether the given Ion data contains shared symbol table imports at the beginning of the stream.
     * @param bytes the Ion data to test.
     * @return true if the given Ion data contains shared symbol table imports at the beginning of the stream;
     *   otherwise, false.
     * @throws IOException if thrown when parsing the data.
     */
    private static boolean streamIncludesImports(byte[] bytes) throws IOException {
        try (IonReader reader = IonReaderBuilder.standard().build(new ByteArrayInputStream(bytes))) {
            reader.next();
            SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
            return imports != null && imports.length > 0;
        }
    }

    /**
     * Asserts that the given Ion data declares the shared symbol table imports specified in the given file at the
     * beginning of the stream.
     * @param expectedImportsFile file containing shared symbol tables.
     * @param bytes the Ion data to test.
     * @throws IOException if thrown when parsing the imports file or the Ion data.
     */
    private static void assertImportsEqual(String expectedImportsFile, byte[] bytes) throws IOException {
        try (InputStream input = new ByteArrayInputStream(bytes)) {
            assertTrue(IonUtilities.importsEqual(expectedImportsFile, input));
        }
    }

    /**
     * Asserts that a read task executes as expected. This includes assertions that temporary files are created only
     * when expected and are always cleaned up, that conversions between formats are correct and happen only when
     * expected, and that all phases of the read task complete without errors.
     * @param inputFileName the file to test.
     * @param optionsCombination a combination of read command options.
     * @param expectedFormat the format of the data that is expected to be tested.
     * @param isConversionRequired false if the task is expected to be able to read the input file without conversion
     *                             or copying; otherwise, false.
     * @throws Exception if an unexpected error occurs.
     */
    private static void assertReadTaskExecutesCorrectly(
        String inputFileName,
        ReadOptionsCombination optionsCombination,
        Format expectedFormat,
        boolean isConversionRequired
    ) throws Exception {
        Path inputPath = fileInTestDirectory(inputFileName);
        MeasurableReadTask task = (MeasurableReadTask) optionsCombination.createMeasurableTask(
            inputPath
        );
        task.setUpTrial();
        byte[] streamBytes;
        if (optionsCombination.ioType == IoType.FILE) {
            assertNull(task.buffer);
            assertEquals(!isConversionRequired, inputPath.toFile().equals(task.inputFile));
            streamBytes = Files.readAllBytes(task.inputFile.toPath());
        } else {
            assertNotNull(task.buffer);
            streamBytes = task.buffer;
        }
        assertFormat(streamBytes, expectedFormat);
        if (expectedFormat.canParse(Format.classify(inputPath))) {
            // If this is a conversion between two formats with the same data model (e.g. text Ion to binary Ion),
            // then they should compare equivalent.
            assertDataEquals(expectedFormat, inputPath.toFile(), streamBytes, optionsCombination);
        }
        if (Format.ION_BINARY.canParse(expectedFormat)) {
            assertEquals(optionsCombination.importsForBenchmarkFile != null, streamIncludesImports(streamBytes));
        } else {
            assertNull(optionsCombination.importsForBenchmarkFile);
        }
        if (optionsCombination.importsForBenchmarkFile != null) {
            assertImportsEqual(optionsCombination.importsForBenchmarkFile, streamBytes);
        }
        // Ensure that the task executes without error.
        MeasurableTask.Task callable = task.getTask();
        task.setUpIteration();
        callable.run(SideEffectConsumer.NO_OP);
        task.tearDownIteration();
        task.tearDownTrial();
        if (isConversionRequired && optionsCombination.ioType == IoType.FILE) {
            // Conversion was required, so the inputFile is a trial-specific temporary file. Ensure it is deleted.
            assertFalse(task.inputFile.exists());
        }
        // Verify that the original file was not deleted.
        assertTrue(inputPath.toFile().exists());
    }

    /**
     * Asserts that a write task executes as expected. This includes assertions that the task writes to file only when
     * expected and that these files are always cleaned up, that the output file or buffer contains data in the expected
     * format that is equivalent to the data in the input file, and that all phases of the write task complete without
     * errors.
     * @param inputFileName the file to test.
     * @param optionsCombination a combination of write command options.
     * @param expectedOutputFormat the expected format of the data to be written.
     * @param expectedIoType the expected IO type.
     * @throws Exception if an unexpected error occurs.
     */
    private static void assertWriteTaskExecutesCorrectly(
        String inputFileName,
        WriteOptionsCombination optionsCombination,
        Format expectedOutputFormat,
        IoType expectedIoType
    ) throws Exception {
        Path inputPath = fileInTestDirectory(inputFileName);
        MeasurableWriteTask<?> task = (MeasurableWriteTask<?>) optionsCombination.createMeasurableTask(
            inputPath
        );
        task.setUpTrial();
        if (expectedOutputFormat.canParse(Format.classify(inputPath))) {
            assertEquals(inputPath.toFile(), task.inputFile);
        } else {
            // If the input file's format cannot be read by parsers of the target format, then the input file must
            // first be converted to the target format.
            assertEquals(expectedOutputFormat, Format.classify(task.inputFile.toPath()));
        }
        // Ensure that the task executes without error.
        MeasurableTask.Task callable = task.getTask();
        task.setUpIteration();
        if (expectedIoType == IoType.FILE) {
            assertTrue(task.currentFile.exists());
            assertNull(task.currentBuffer);
        } else {
            assertNull(task.currentFile);
            // No preparation is needed for the buffer.
            assertNull(task.currentBuffer);
        }
        callable.run(SideEffectConsumer.NO_OP);
        File outputFile = null;
        byte[] outputBytes;
        if (expectedIoType == IoType.FILE) {
            assertNull(task.currentBuffer);
            assertNotNull(task.currentFile);
            outputFile = task.currentFile;
            outputBytes = Files.readAllBytes(task.currentFile.toPath());
        } else {
            assertNull(task.currentFile);
            assertNotNull(task.currentBuffer);
            outputBytes = task.currentBuffer.toByteArray();
        }
        assertFormat(outputBytes, expectedOutputFormat);
        assertDataEquals(expectedOutputFormat, task.inputFile, outputBytes, optionsCombination);
        if (Format.ION_BINARY.canParse(expectedOutputFormat)) {
            assertEquals(optionsCombination.importsForBenchmarkFile != null, streamIncludesImports(outputBytes));
        } else {
            assertNull(optionsCombination.importsForBenchmarkFile);
        }
        if (optionsCombination.importsForBenchmarkFile != null) {
            assertImportsEqual(optionsCombination.importsForBenchmarkFile, outputBytes);
        }
        task.tearDownIteration();
        task.tearDownTrial();
        if (outputFile != null) {
            assertFalse(outputFile.exists());
            assertTrue(task.inputFile.exists());
        }
        assertNull(task.currentFile);
        assertNull(task.currentBuffer);
    }

    /**
     * Parses a list of {@link OptionsCombinationBase} from the given arguments, which are identical to the arguments
     * that would be provided to an invocation of the CLI.
     * @param args the arguments from which to generate options combinations.
     * @param <T> {@link ReadOptionsCombination} for the 'read' command; {@link WriteOptionsCombination} for the 'write'
     *           command.
     * @return a new list.
     * @throws IOException if thrown when parsing the options.
     */
    private static <T extends OptionsCombinationBase> List<T> parseOptionsCombinations(String... args) throws IOException {
        OptionsMatrixBase matrix = OptionsMatrixBase.from(
            Main.parseArguments(args)
        );
        String[] serializedOptionsCombinations = matrix.getSerializedOptionsCombinations();
        List<T> readOptionsCombinations = new ArrayList<>(serializedOptionsCombinations.length);
        for (String serializedOptionsCombination : serializedOptionsCombinations) {
            readOptionsCombinations.add(
                (T) OptionsCombinationBase.from(serializedOptionsCombination)
            );
        }
        return readOptionsCombinations;
    }

    /**
     * Calls {@link #parseOptionsCombinations(String...)}, asserts that the resulting list contains a single element,
     * and returns that element.
     * @param args the arguments from which to generate options combinations.
     * @param <T> {@link ReadOptionsCombination} for the 'read' command; {@link WriteOptionsCombination} for the 'write'
     *           command.
     * @return the only options combination resulting from the given arguments.
     * @throws IOException if thrown when parsing the options.
     */
    private static <T extends OptionsCombinationBase> T parseSingleOptionsCombination(String... args) throws IOException {
        List<T> optionsCombinations = parseOptionsCombinations(args);
        assertEquals(1, optionsCombinations.size());
        return optionsCombinations.get(0);
    }

    /**
     * Tests the given objects for equality, allowing for either or both to be null.
     * @param lhs an Object.
     * @param rhs an Object.
     * @return true if both lhs and rhs are null or `lhs.equals(rhs)` is true; otherwise, false.
     */
    private static boolean nullSafeEquals(Object lhs, Object rhs) {
        if ((lhs == null) != (rhs == null)) {
            return false;
        }
        if (lhs == null) {
            // They're both null.
            return true;
        }
        return lhs.equals(rhs);
    }

    @Before
    public void prepareTemporaryDirectory() throws IOException {
        TemporaryFiles.prepareTempDirectory();
    }

    @After
    public void cleanUpTemporaryDirectory() throws IOException {
        TemporaryFiles.cleanUpTempDirectory();
    }

    @Test
    public void defaultRead() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination("read", "binaryStructs.10n");
        ExpectedReadOptionsCombination.defaultOptions().assertOptionsEqual(optionsCombination);
        // No conversion is required because the input is already binary Ion.
        assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, false);
        // Conversion is required because the input is text Ion but binary Ion is requested.
        assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, true);
    }

    @Test
    public void defaultWrite() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination("write", "binaryStructs.10n");
        ExpectedWriteOptionsCombination.defaultOptions().assertOptionsEqual(optionsCombination);
        assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
        assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
    }

    @Test
    public void writeTextUsingDom() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--format",
            "ion_text",
            "--api",
            "dom",
            "--io-type",
            "buffer",
            "--io-type",
            "file",
            "textStructs.ion"
        );
        // There were IoTypes requested.
        assertEquals(2, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions()
            .api(API.DOM)
            .format(Format.ION_TEXT)
            .ioType(IoType.BUFFER)
        );
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions()
            .api(API.DOM)
            .format(Format.ION_TEXT)
            .ioType(IoType.FILE)
        );

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> {
                return candidate.api == API.DOM
                    && candidate.format == Format.ION_TEXT
                    && candidate.ioType == optionsCombination.ioType;
            });

            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_TEXT, optionsCombination.ioType);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_TEXT, optionsCombination.ioType);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBothTextAndIonUsingBothDomAndReader() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "--io-type",
            "buffer",
            "--api",
            "dom",
            "--api",
            "streaming",
            "binaryStructs.10n"
        );
        // There were two Formats and two IonAPIs requested; four combinations.
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .api(API.DOM)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .api(API.STREAMING)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_BINARY)
            .api(API.DOM)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_BINARY)
            .api(API.STREAMING)
        );
        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.format == optionsCombination.format
                && candidate.api == optionsCombination.api);
            assertReadTaskExecutesCorrectly(
                "binaryStructs.10n",
                optionsCombination,
                optionsCombination.format,
                optionsCombination.format != Format.ION_BINARY
            );
            assertReadTaskExecutesCorrectly(
                "textStructs.ion",
                optionsCombination,
                optionsCombination.format,
                optionsCombination.format != Format.ION_TEXT
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBinaryWithLimit() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "--io-type",
            "file",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "binaryStructs.10n"
        );
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_BINARY).ioType(IoType.BUFFER));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_TEXT).ioType(IoType.BUFFER));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_BINARY).ioType(IoType.FILE));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_TEXT).ioType(IoType.FILE));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.format == optionsCombination.format && candidate.ioType == optionsCombination.ioType);
            assertEquals(1, optionsCombination.limit);

            assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, optionsCombination.format, true);
            assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, optionsCombination.format, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBinaryWithLimitFromFileUsingDom() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--limit",
            "1",
            "--io-type",
            "file",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "--api",
            "dom",
            "binaryStructs.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).api(API.DOM).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).api(API.DOM).format(Format.ION_TEXT));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.format == optionsCombination.format);
            assertEquals(1, optionsCombination.limit);

            assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, optionsCombination.format, true);
            assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, optionsCombination.format, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryWithLimitUsingWriterAndDOM() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--limit",
            "1",
            "--api",
            "dom",
            "--api",
            "streaming",
            "binaryStructs.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().limit(1).api(API.DOM));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().limit(1).api(API.STREAMING));

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.api == optionsCombination.api);
            assertEquals(1, optionsCombination.limit);

            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void profileWithMultipleCombinationsRaisesError() {
        assertThrows(
            IllegalArgumentException.class,
            () -> parseOptionsCombinations(
                "read",
                "--profile",
                "--api",
                "dom",
                "--api",
                "streaming",
                "binaryStructs.10n"
            )
        );
    }

    @Test
    public void readBinaryWithVariousFlushPeriodsAndPreallocations() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-length-preallocation",
            "0",
            "--ion-length-preallocation",
            "2",
            "--ion-length-preallocation",
            "auto",
            "--ion-flush-period",
            "1",
            "--ion-flush-period",
            "2",
            "--ion-flush-period",
            "auto",
            "binaryStructs.10n"
        );
        assertEquals(9, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(9);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(0).flushPeriod(1));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(0).flushPeriod(2));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(0).flushPeriod(null));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(2).flushPeriod(1));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(2).flushPeriod(2));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(2).flushPeriod(null));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(null).flushPeriod(1));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(null).flushPeriod(2));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().preallocation(null).flushPeriod(null));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.preallocation, optionsCombination.preallocation)
                && nullSafeEquals(candidate.flushPeriod, optionsCombination.flushPeriod));
            boolean isConversionRequired = optionsCombination.preallocation != null || optionsCombination.flushPeriod != null;
            assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, isConversionRequired);
            assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryWithVariousFlushPeriodsAndPreallocations() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--ion-length-preallocation",
            "0",
            "--ion-length-preallocation",
            "2",
            "--ion-length-preallocation",
            "auto",
            "--ion-flush-period",
            "1",
            "--ion-flush-period",
            "2",
            "--ion-flush-period",
            "auto",
            "binaryStructs.10n"
        );
        assertEquals(9, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(9);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(0).flushPeriod(1));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(0).flushPeriod(2));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(0).flushPeriod(null));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(2).flushPeriod(1));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(2).flushPeriod(2));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(2).flushPeriod(null));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(null).flushPeriod(1));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(null).flushPeriod(2));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().preallocation(null).flushPeriod(null));

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.preallocation, optionsCombination.preallocation)
                && nullSafeEquals(candidate.flushPeriod, optionsCombination.flushPeriod));
            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void binarySparseReadFromFile() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--paths",
            fileInTestDirectory("paths.ion").toString(),
            "binaryStructs.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .paths(Arrays.asList("(foo)", "(bar 1)"))
            .assertOptionsEqual(optionsCombination);
        assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, false);
        assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, true);
    }

    @Test
    public void textSparseReadFromBuffer() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--paths",
            fileInTestDirectory("paths.ion").toString(),
            "--io-type",
            "buffer",
            "--format",
            "ion_text",
            "binaryStructs.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .paths(Arrays.asList("(foo)", "(bar 1)"))
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .assertOptionsEqual(optionsCombination);
        assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_TEXT, true);
        assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_TEXT, true);
    }

    @Test
    public void readBinaryWithAndWithoutImports() throws Exception {
        String importsFileName = fileInTestDirectory("importsVersion1.ion").toString();
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-imports-for-benchmark",
            importsFileName,
            "--ion-imports-for-benchmark",
            "none",
            "--io-type",
            "buffer",
            "--ion-use-symbol-tokens",
            "true",
            "binaryStructs.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, optionsCombination.importsForBenchmarkFile != null);
            assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBinaryFromDataWithImports() throws Exception {
        // During the read benchmark, the reader must use the SymbolToken APIs when no catalog is provided because
        // all symbols will have unknown text.
        String importsFileName = fileInTestDirectory("importsVersion1.ion").toString();
        String importsV2FileName = fileInTestDirectory("importsVersion2.ion").toString();
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-imports-for-input",
            importsFileName,
            "--ion-imports-for-benchmark",
            importsFileName,
            "--ion-imports-for-benchmark",
            importsV2FileName,
            "--ion-imports-for-benchmark",
            "none",
            "--ion-reader",
            "non_incremental",
            "--ion-use-symbol-tokens",
            "true",
            "binaryStructsWithImports.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsV2FileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            // binaryStructsWithImports.10n is already encoded with the same symbol tables declared in importsVersion1.ion, so it does
            // not need to be re-encoded when the imports for benchmark is importsVersion1.ion.
            assertReadTaskExecutesCorrectly("binaryStructsWithImports.10n", optionsCombination, Format.ION_BINARY, !importsFileName.equals(optionsCombination.importsForBenchmarkFile));
            assertReadTaskExecutesCorrectly("textStructsWithImports.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryWithAndWithoutImports() throws Exception {
        String importsFileName = fileInTestDirectory("importsVersion1.ion").toString();
        String importsV2FileName = fileInTestDirectory("importsVersion2.ion").toString();
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--ion-imports-for-benchmark",
            importsFileName,
            "--ion-imports-for-benchmark",
            importsV2FileName,
            "--ion-imports-for-benchmark",
            "none",
            "binaryStructs.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().importsForBenchmarkFile(importsV2FileName));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryFromDataWithImports() throws Exception {
        String importsFileName = fileInTestDirectory("importsVersion1.ion").toString();
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--ion-imports-for-input",
            importsFileName,
            "--ion-imports-for-benchmark",
            "none",
            "--io-type",
            "buffer",
            "--ion-use-symbol-tokens",
            "true",
            "binaryStructsWithImports.10n"
        );
        ExpectedWriteOptionsCombination.defaultOptions()
            .importsForInputFile(importsFileName)
            .importsForBenchmarkFile(null)
            .ioType(IoType.BUFFER)
            .useSymbolTokens(true)
            .assertOptionsEqual(optionsCombination);

        assertWriteTaskExecutesCorrectly("binaryStructsWithImports.10n", optionsCombination, Format.ION_BINARY, IoType.BUFFER);
        assertWriteTaskExecutesCorrectly("textStructsWithImports.ion", optionsCombination, Format.ION_BINARY, IoType.BUFFER);
    }

    @Test
    public void importsForBenchmarkAuto() throws Exception {
        String importsFileName = fileInTestDirectory("importsVersion1.ion").toString();
        WriteOptionsCombination combination1 = parseSingleOptionsCombination(
            "write",
            "--ion-imports-for-input",
            importsFileName,
            "binaryStructsWithImports.10n"
        );
        ExpectedWriteOptionsCombination.defaultOptions()
            .importsForInputFile(importsFileName)
            .importsForBenchmarkFile(importsFileName)
            .assertOptionsEqual(combination1);

        WriteOptionsCombination combination2 = parseSingleOptionsCombination(
            "write",
            "--ion-imports-for-input",
            "none",
            "--ion-imports-for-benchmark",
            "auto",
            "binaryStructsWithImports.10n"
        );
        ExpectedWriteOptionsCombination.defaultOptions()
            .importsForInputFile(null)
            .importsForBenchmarkFile(null)
            .assertOptionsEqual(combination2);

        ReadOptionsCombination combination3 = parseSingleOptionsCombination(
            "read",
            "--ion-imports-for-input",
            importsFileName,
            "--ion-imports-for-benchmark",
            "auto",
            "binaryStructsWithImports.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .importsForInputFile(importsFileName)
            .importsForBenchmarkFile(importsFileName)
            .assertOptionsEqual(combination3);

        ReadOptionsCombination combination4 = parseSingleOptionsCombination(
            "read",
            "--ion-imports-for-input",
            "none",
            "binaryStructsWithImports.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .importsForInputFile(null)
            .importsForBenchmarkFile(null)
            .assertOptionsEqual(combination4);
    }

    @Test
    public void readBinaryFromDataWithImportsWithoutProvidingCatalogRaisesError() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "binaryStructsWithImports.10n"
        );
        Path inputPath = fileInTestDirectory("binaryStructsWithImports.10n");
        assertThrows(IllegalArgumentException.class, () -> optionsCombination.createMeasurableTask(inputPath));
    }

    @Test
    public void writeBinaryFromDataWithImportsWithoutProvidingCatalogRaisesError() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "binaryStructsWithImports.10n"
        );
        Path inputPath = fileInTestDirectory("binaryStructsWithImports.10n");
        assertThrows(IllegalArgumentException.class, () -> optionsCombination.createMeasurableTask(inputPath));
    }

    @Test
    public void readBinaryFloatWidths() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-float-width",
            "32",
            "--ion-float-width",
            "64",
            "--ion-float-width",
            "auto",
            "binaryStructs.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().floatWidth(32));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().floatWidth(64));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.floatWidth, optionsCombination.floatWidth));

            assertReadTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, optionsCombination.floatWidth != null);
            assertReadTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryFloatWidths() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--ion-float-width",
            "32",
            "--ion-float-width",
            "64",
            "--ion-float-width",
            "auto",
            "binaryStructs.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().floatWidth(32));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().floatWidth(64));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.floatWidth, optionsCombination.floatWidth));

            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void invalidFloatWidthRaisesError() {
        assertThrows(
            IllegalArgumentException.class,
            () -> parseSingleOptionsCombination(
            "write",
            "--ion-float-width",
            "99",
            "binaryStructs.10n"
            )
        );
    }

    @Test
    public void ionWriterBlockSize() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--ion-writer-block-size",
            "16",
            "--ion-writer-block-size",
            "1024",
            "--ion-writer-block-size",
            "auto",
            "binaryStructs.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);

        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ionWriterBlockSize(16));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ionWriterBlockSize(1024));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.ionWriterBlockSize, optionsCombination.ionWriterBlockSize));

            assertWriteTaskExecutesCorrectly("binaryStructs.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textStructs.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeAllTypes() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--format",
            "ion_binary",
            "--format",
            "ion_text",
            "binaryAllTypes.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(2);

        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().format(Format.ION_TEXT));

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.format, optionsCombination.format));

            assertWriteTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, optionsCombination.format, IoType.FILE);
            assertWriteTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, optionsCombination.format, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readAllTypes() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--format",
            "ion_binary",
            "--format",
            "ion_text",
            "--ion-reader",
            "non_incremental",
            "--ion-reader",
            "incremental",
            "binaryAllTypes.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);

        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().readerType(IonReaderType.NON_INCREMENTAL).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().readerType(IonReaderType.INCREMENTAL).format(Format.ION_BINARY));
        // --ion-reader does not apply to the text format.
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.ION_TEXT));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> {
                return nullSafeEquals(candidate.format, optionsCombination.format)
                    && candidate.readerType == optionsCombination.readerType;
            });

            assertReadTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_TEXT);
            assertReadTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_BINARY);

        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readWithVariousIoBufferSizes() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--io-buffer-size",
            "32",
            "--io-buffer-size",
            "16384",
            "--io-buffer-size",
            "auto",
            "binaryAllTypes.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);

        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().ioBufferSize(32));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().ioBufferSize(16384));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> nullSafeEquals(candidate.ioBufferSize, optionsCombination.ioBufferSize));

            assertReadTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_TEXT);
            assertReadTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_BINARY);
        }

        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeWithVariousBufferSizes() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--io-buffer-size",
            "32",
            "--io-buffer-size",
            "16384",
            "--io-buffer-size",
            "auto",
            "--io-type",
            "file",
            "--io-type",
            "buffer",
            "binaryAllTypes.10n"
        );
        assertEquals(4, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(4);

        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ioType(IoType.FILE).ioBufferSize(32));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ioType(IoType.FILE).ioBufferSize(16384));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ioType(IoType.FILE));
        // --io-buffer-size does not apply to the BUFFER IoType.
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ioType(IoType.BUFFER));

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> {
                return nullSafeEquals(candidate.ioBufferSize, optionsCombination.ioBufferSize)
                    && candidate.ioType == optionsCombination.ioType;
            });

            assertWriteTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, Format.ION_BINARY, optionsCombination.ioType);
            assertWriteTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, Format.ION_BINARY, optionsCombination.ioType);
        }

        assertTrue(expectedCombinations.isEmpty());
    }

    @Ignore // TODO amzn/ion-java-benchmark-cli/issues/2
    @Test
    public void readUsingLobChunks() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-use-lob-chunks",
            "true",
            "--ion-use-lob-chunks",
            "false",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "binaryLargeLobs.10n"
        );
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);

        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useLobChunks(true).format(Format.ION_TEXT));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useLobChunks(false).format(Format.ION_TEXT));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useLobChunks(true).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useLobChunks(false).format(Format.ION_BINARY));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.useLobChunks == optionsCombination.useLobChunks && candidate.format == optionsCombination.format);

            assertReadTaskExecutesCorrectly("binaryLargeLobs.10n", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_TEXT);
            assertReadTaskExecutesCorrectly("textLargeLobs.ion", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_BINARY);
        }

        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readUsingBigDecimals() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-use-big-decimals",
            "true",
            "--ion-use-big-decimals",
            "false",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "binaryLargeLobs.10n"
        );
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);

        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useBigDecimals(true).format(Format.ION_TEXT));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useBigDecimals(false).format(Format.ION_TEXT));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useBigDecimals(true).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().useBigDecimals(false).format(Format.ION_BINARY));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.useBigDecimals == optionsCombination.ionUseBigDecimals && candidate.format == optionsCombination.format);

            assertReadTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_TEXT);
            assertReadTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_BINARY);
        }

        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readWithCustomIncrementalBufferSize() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--ion-reader-buffer-size",
            "auto",
            "--ion-reader-buffer-size",
            "128",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "--ion-reader",
            "non_incremental",
            "--ion-reader",
            "incremental",
            "binaryLargeLobs.10n"
        );
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.ION_TEXT));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().readerType(IonReaderType.INCREMENTAL).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().readerType(IonReaderType.NON_INCREMENTAL).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().initialBufferSize(128).readerType(IonReaderType.INCREMENTAL).format(Format.ION_BINARY));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> {
                return nullSafeEquals(candidate.initialBufferSize, optionsCombination.initialBufferSize) &&
                    candidate.format == optionsCombination.format &&
                    candidate.readerType == optionsCombination.readerType;
            });

            assertReadTaskExecutesCorrectly("binaryAllTypes.10n", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_TEXT);
            assertReadTaskExecutesCorrectly("textAllTypes.ion", optionsCombination, optionsCombination.format, optionsCombination.format == Format.ION_BINARY);
        }

        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeJson() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "json",
            "objects.json"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            IoType.FILE
        );
    }

    @Test
    public void readJson() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "object.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            false
        );
    }

    @Test
    public void writeJsonToBufferWithLimit() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "json",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "objects.json"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            IoType.BUFFER
        );
    }

    @Test
    public void readJsonToBufferWithLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "objects.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            true
        );
    }

    @Test
    public void writeJsonFromDomWithLimit() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "json",
            "--limit",
            "1",
            "--api",
            "dom",
            "objects.json"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            IoType.FILE
        );
    }

    @Test
    public void readJsonFromDomWithoutLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "--api",
            "dom",
            "--io-type",
            "buffer",
            "objects.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            false
        );
    }

    @Test
    public void readJsonFromDomWithLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "--limit",
            "1",
            "--api",
            "dom",
            "--io-type",
            "file",
            "objects.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.JSON,
            true
        );
    }

    @Test
    public void writeJsonFromIon() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "json",
            "textAllTypes.ion"
        );
        assertWriteTaskExecutesCorrectly(
            "textAllTypes.ion",
            optionsCombination,
            Format.JSON,
            IoType.FILE
        );
    }

    @Test
    public void readJsonFromIon() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "textAllTypes.ion"
        );
        assertReadTaskExecutesCorrectly(
            "textAllTypes.ion",
            optionsCombination,
            Format.JSON,
            true
        );
    }

    @Test
    public void writeIonFromJson() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "ion_binary",
            "objects.json"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.ION_BINARY,
            IoType.FILE
        );
    }

    @Test
    public void readIonFromJson() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "ion_binary",
            "objects.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.ION_BINARY,
            true
        );
    }

    @Test
    public void traverseJsonDoesNotFail() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--format",
            "json",
            "--paths",
            fileInTestDirectory("paths.ion").toString(),
            "--io-type",
            "buffer",
            "--io-type",
            "file",
            "objects.json"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.JSON).limit(1).api(API.DOM).ioType(IoType.BUFFER));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.JSON).limit(1).api(API.DOM).ioType(IoType.FILE));
        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.ioType == optionsCombination.ioType);
            assertReadTaskExecutesCorrectly(
                "objects.json",
                optionsCombination,
                Format.JSON,
                false
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readJsonWithAndWithoutBigDecimals() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--format",
            "json",
            "--json-use-big-decimals",
            "true",
            "--json-use-big-decimals",
            "false",
            "objects.json"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.JSON).jsonUseBigDecimals(true));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.JSON).jsonUseBigDecimals(false));
        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.jsonUseBigDecimals == optionsCombination.jsonUseBigDecimals);
            assertReadTaskExecutesCorrectly(
                "objects.json",
                optionsCombination,
                Format.JSON,
                false
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeJsonWithAndWithoutBigDecimals() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--format",
            "json",
            "--json-use-big-decimals",
            "true",
            "--json-use-big-decimals",
            "false",
            "objects.json"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().format(Format.JSON).jsonUseBigDecimals(true));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().format(Format.JSON).jsonUseBigDecimals(false));
        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.jsonUseBigDecimals == optionsCombination.jsonUseBigDecimals);
            assertWriteTaskExecutesCorrectly(
                "objects.json",
                optionsCombination,
                Format.JSON,
                IoType.FILE
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeCbor() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "cbor",
            "objects.cbor"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            IoType.FILE
        );
    }

    @Test
    public void readCbor() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "object.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            false
        );
    }

    @Test
    public void writeCborToBufferWithLimit() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "cbor",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "objects.cbor"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            IoType.BUFFER
        );
    }

    @Test
    public void readCborToBufferWithLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "objects.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            true
        );
    }

    @Test
    public void writeCborFromDomWithLimit() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "cbor",
            "--limit",
            "1",
            "--api",
            "dom",
            "objects.cbor"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            IoType.FILE
        );
    }

    @Test
    public void readCborFromDomWithoutLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "--api",
            "dom",
            "--io-type",
            "buffer",
            "objects.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            false
        );
    }

    @Test
    public void readCborFromDomWithLimit() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "--limit",
            "1",
            "--api",
            "dom",
            "--io-type",
            "file",
            "objects.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.CBOR,
            true
        );
    }

    @Test
    public void writeCborFromIon() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "cbor",
            "textAllTypes.ion"
        );
        assertWriteTaskExecutesCorrectly(
            "textAllTypes.ion",
            optionsCombination,
            Format.CBOR,
            IoType.FILE
        );
    }

    @Test
    public void readCborFromIon() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "textAllTypes.ion"
        );
        assertReadTaskExecutesCorrectly(
            "textAllTypes.ion",
            optionsCombination,
            Format.CBOR,
            true
        );
    }

    @Test
    public void writeIonFromCbor() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "ion_binary",
            "objects.cbor"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.ION_BINARY,
            IoType.FILE
        );
    }

    @Test
    public void readIonFromCbor() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "ion_binary",
            "objects.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.ION_BINARY,
            true
        );
    }

    @Test
    public void writeCborFromJson() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "cbor",
            "objects.json"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.CBOR,
            IoType.FILE
        );
    }

    @Test
    public void readCborFromJson() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "cbor",
            "objects.json"
        );
        assertReadTaskExecutesCorrectly(
            "objects.json",
            optionsCombination,
            Format.CBOR,
            true
        );
    }

    @Test
    public void writeJsonFromCbor() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "json",
            "objects.cbor"
        );
        assertWriteTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.JSON,
            IoType.FILE
        );
    }

    @Test
    public void readJsonFromCbor() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--format",
            "json",
            "objects.cbor"
        );
        assertReadTaskExecutesCorrectly(
            "objects.cbor",
            optionsCombination,
            Format.JSON,
            true
        );
    }

    @Test
    public void traverseCborDoesNotFail() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--format",
            "cbor",
            "--paths",
            fileInTestDirectory("paths.ion").toString(),
            "--io-type",
            "buffer",
            "--io-type",
            "file",
            "objects.cbor"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.CBOR).limit(1).api(API.DOM).ioType(IoType.BUFFER));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().format(Format.CBOR).limit(1).api(API.DOM).ioType(IoType.FILE));
        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(candidate -> candidate.ioType == optionsCombination.ioType);
            assertReadTaskExecutesCorrectly(
                "objects.cbor",
                optionsCombination,
                Format.CBOR,
                false
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }
}
