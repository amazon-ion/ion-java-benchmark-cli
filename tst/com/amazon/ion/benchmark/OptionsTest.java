package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.Equivalence;
import com.amazon.ion.util.IonStreamUtils;
import org.junit.After;
import org.junit.Before;
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
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OptionsTest {

    private static Path fileInTestDirectory(String simpleName) {
        return Paths.get("tst", "com", "amazon", "ion", "benchmark", simpleName);
    }

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

    private static void assertFormat(byte[] buffer, Format expectedFormat) {
        assertEquals(expectedFormat == Format.ION_BINARY, IonStreamUtils.isIonBinary(buffer));
    }

    private abstract static class ExpectedOptionsCombinationBase<T extends ExpectedOptionsCombinationBase, U extends OptionsCombinationBase> {
        Integer preallocation = null;
        Integer flushPeriod = null;
        Format format = Format.ION_BINARY;
        IonAPI api = IonAPI.STREAMING;
        IoType ioType = IoType.FILE;
        String importsForInputFile = null;
        String importsForBenchmarkFile = null;
        int limit = Integer.MAX_VALUE;
        boolean useSymbolTokens = false;
        Integer floatWidth = null;

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

        final T api(IonAPI api) {
            this.api = api;
            return (T) this;
        }

        final T ioType(IoType ioType) {
            this.ioType = ioType;
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

        void assertOptionsEqual(U that) {
            assertEquals(flushPeriod, that.flushPeriod);
            assertEquals(api, that.api);
            assertEquals(format, that.format);
            assertEquals(limit, that.limit);
            assertEquals(preallocation, that.preallocation);
            assertEquals(importsForInputFile, that.importsForInputFile);
            assertEquals(importsForBenchmarkFile, that.importsForBenchmarkFile);
            assertEquals(ioType, that.ioType);
            assertEquals(floatWidth, that.floatWidth);
        }
    }

    private static class ExpectedReadOptionsCombination
        extends ExpectedOptionsCombinationBase<ExpectedReadOptionsCombination, ReadOptionsCombination> {

        List<String> paths = null;
        IonReaderType readerType = IonReaderType.NON_BLOCKING;

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

        @Override
        void assertOptionsEqual(ReadOptionsCombination that) {
            super.assertOptionsEqual(that);
            assertEquals(paths, that.paths);
            assertEquals(readerType, that.readerType);
        }
    }

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

    @Before
    public void prepareTemporaryDirectory() throws IOException {
        TemporaryFiles.prepareTempDirectory();
    }

    @After
    public void cleanUpTemporaryDirectory() throws IOException {
        TemporaryFiles.cleanUpTempDirectory();
    }

    private static boolean streamIncludesImports(byte[] bytes) throws IOException {
        try (IonReader reader = IonReaderBuilder.standard().build(new ByteArrayInputStream(bytes))) {
            reader.next();
            SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
            return imports != null && imports.length > 0;
        }
    }

    private static void assertImportsEqual(String expectedImportsFile, byte[] bytes) throws IOException {
        try (InputStream input = new ByteArrayInputStream(bytes)) {
            assertTrue(IonUtilities.importsEqual(expectedImportsFile, input));
        }
    }

    private static void assertReadTaskExecutesCorrectly(
        String inputFileName,
        ReadOptionsCombination optionsCombination,
        Format expectedFormat,
        boolean isConversionRequired
    ) throws Exception {
        Path inputPath = fileInTestDirectory(inputFileName);
        IonMeasurableReadTask task = (IonMeasurableReadTask) optionsCombination.createMeasurableTask(
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
        assertIonEquals(inputPath.toFile(), streamBytes, optionsCombination);
        assertEquals(optionsCombination.importsForBenchmarkFile != null, streamIncludesImports(streamBytes));
        if (optionsCombination.importsForBenchmarkFile != null) {
            assertImportsEqual(optionsCombination.importsForBenchmarkFile, streamBytes);
        }
        // Ensure that the task executes without error.
        Callable<Void> callable = task.getTask();
        task.setUpIteration();
        callable.call();
        task.tearDownIteration();
        task.tearDownTrial();
        if (isConversionRequired && optionsCombination.ioType == IoType.FILE) {
            // Conversion was required, so the inputFile is a trial-specific temporary file. Ensure it is deleted.
            assertFalse(task.inputFile.exists());
        }
        // Verify that the original file was not deleted.
        assertTrue(inputPath.toFile().exists());
    }

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

    private static <T extends OptionsCombinationBase> T parseSingleOptionsCombination(String... args) throws IOException {
        List<T> optionsCombinations = parseOptionsCombinations(args);
        assertEquals(1, optionsCombinations.size());
        return optionsCombinations.get(0);
    }

    @Test
    public void defaultRead() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination("read", "input1.10n");
        ExpectedReadOptionsCombination.defaultOptions().assertOptionsEqual(optionsCombination);
        // No conversion is required because the input is already binary Ion.
        assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, false);
        // Conversion is required because the input is text Ion but binary Ion is requested.
        assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, true);
    }

    private static void assertWriteTaskExecutesCorrectly(
        String inputFileName,
        WriteOptionsCombination optionsCombination,
        Format expectedOutputFormat,
        IoType expectedIoType
    ) throws Exception {
        Path inputPath = fileInTestDirectory(inputFileName);
        IonMeasurableWriteTask task = (IonMeasurableWriteTask) optionsCombination.createMeasurableTask(
            inputPath
        );
        task.setUpTrial();
        assertEquals(inputPath.toFile(), task.inputFile);
        // Ensure that the task executes without error.
        Callable<Void> callable = task.getTask();
        task.setUpIteration();
        if (expectedIoType == IoType.FILE) {
            assertTrue(task.currentFile.exists());
            assertNull(task.currentBuffer);
        } else {
            assertNull(task.currentFile);
            // No preparation is needed for the buffer.
            assertNull(task.currentBuffer);
        }
        callable.call();
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
        assertIonEquals(inputPath.toFile(), outputBytes, optionsCombination);
        assertEquals(optionsCombination.importsForBenchmarkFile != null, streamIncludesImports(outputBytes));
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

    @Test
    public void defaultWrite() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination("write", "input1.10n");
        ExpectedWriteOptionsCombination.defaultOptions().assertOptionsEqual(optionsCombination);
        assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
        assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
    }

    @Test
    public void writeTextToBufferUsingDom() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "--format",
            "ion_text",
            "--ion-api",
            "dom",
            "--io-type",
            "buffer",
            "input1.ion"
        );
        ExpectedWriteOptionsCombination.defaultOptions()
            .api(IonAPI.DOM)
            .format(Format.ION_TEXT)
            .ioType(IoType.BUFFER)
            .assertOptionsEqual(optionsCombination);

        assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_TEXT, IoType.BUFFER);
        assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_TEXT, IoType.BUFFER);
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
            "--ion-api",
            "dom",
            "--ion-api",
            "streaming",
            "input1.10n"
        );
        // There were two Formats and two IonAPIs requested; four combinations.
        assertEquals(4, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(4);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .api(IonAPI.DOM)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .api(IonAPI.STREAMING)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_BINARY)
            .api(IonAPI.DOM)
        );
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions()
            .ioType(IoType.BUFFER)
            .format(Format.ION_BINARY)
            .api(IonAPI.STREAMING)
        );
        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> expectedCandidate.format == optionsCombination.format
                && expectedCandidate.api == optionsCombination.api);
            assertReadTaskExecutesCorrectly(
                "input1.10n",
                optionsCombination,
                optionsCombination.format,
                optionsCombination.format != Format.ION_BINARY
            );
            assertReadTaskExecutesCorrectly(
                "input1.ion",
                optionsCombination,
                optionsCombination.format,
                optionsCombination.format != Format.ION_TEXT
            );
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBinaryWithLimitFromBuffer() throws Exception {
        List<ReadOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "read",
            "--limit",
            "1",
            "--io-type",
            "buffer",
            "--format",
            "ion_text",
            "--format",
            "ion_binary",
            "input1.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).format(Format.ION_TEXT));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> expectedCandidate.format == optionsCombination.format);
            assertEquals(1, optionsCombination.limit);

            assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, optionsCombination.format, true);
            assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, optionsCombination.format, true);
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
            "--ion-api",
            "dom",
            "input1.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).api(IonAPI.DOM).format(Format.ION_BINARY));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().limit(1).api(IonAPI.DOM).format(Format.ION_TEXT));

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> expectedCandidate.format == optionsCombination.format);
            assertEquals(1, optionsCombination.limit);

            assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, optionsCombination.format, true);
            assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, optionsCombination.format, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryWithLimitUsingWriterAndDOM() throws Exception {
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--limit",
            "1",
            "--ion-api",
            "dom",
            "--ion-api",
            "streaming",
            "input1.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().limit(1).api(IonAPI.DOM));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().limit(1).api(IonAPI.STREAMING));

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> expectedCandidate.api == optionsCombination.api);
            assertEquals(1, optionsCombination.limit);

            assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
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
                "--ion-api",
                "dom",
                "--ion-api",
                "streaming",
                "input1.10n"
            )
        );
    }

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
            "input1.10n"
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
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.preallocation, optionsCombination.preallocation)
                && nullSafeEquals(expectedCandidate.flushPeriod, optionsCombination.flushPeriod));
            boolean isConversionRequired = optionsCombination.preallocation != null || optionsCombination.flushPeriod != null;
            assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, isConversionRequired);
            assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, true);
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
            "input1.10n"
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
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.preallocation, optionsCombination.preallocation)
                && nullSafeEquals(expectedCandidate.flushPeriod, optionsCombination.flushPeriod));
            assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void binarySparseReadFromFile() throws Exception {
        ReadOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "read",
            "--paths",
            fileInTestDirectory("paths.ion").toString(),
            "input1.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .paths(Arrays.asList("(foo)", "(bar 1)"))
            .assertOptionsEqual(optionsCombination);
        assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, false);
        assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, true);
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
            "input1.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .paths(Arrays.asList("(foo)", "(bar 1)"))
            .ioType(IoType.BUFFER)
            .format(Format.ION_TEXT)
            .assertOptionsEqual(optionsCombination);
        assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_TEXT, true);
        assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_TEXT, true);
    }

    @Test
    public void readBinaryWithAndWithoutImports() throws Exception {
        String importsFileName = fileInTestDirectory("imports.ion").toString();
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
            "input1.10n"
        );
        assertEquals(2, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(2);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, optionsCombination.importsForBenchmarkFile != null);
            assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void readBinaryFromDataWithImports() throws Exception {
        // During the read benchmark, the reader must use the SymbolToken APIs when no catalog is provided because
        // all symbols will have unknown text.
        String importsFileName = fileInTestDirectory("imports.ion").toString();
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
            "blocking",
            "--ion-use-symbol-tokens",
            "true",
            "binaryWithImports.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().importsForBenchmarkFile(importsV2FileName));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            // binaryWithImports.10n is already encoded with the same symbol tables declared in imports.ion, so it does
            // not need to be re-encoded when the imports for benchmark is imports.ion.
            assertReadTaskExecutesCorrectly("binaryWithImports.10n", optionsCombination, Format.ION_BINARY, !importsFileName.equals(optionsCombination.importsForBenchmarkFile));
            assertReadTaskExecutesCorrectly("textWithImports.ion", optionsCombination, Format.ION_BINARY, true);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryWithAndWithoutImports() throws Exception {
        String importsFileName = fileInTestDirectory("imports.ion").toString();
        String importsV2FileName = fileInTestDirectory("importsVersion2.ion").toString();
        List<WriteOptionsCombination> optionsCombinations = parseOptionsCombinations(
            "write",
            "--ion-imports-for-benchmark",
            importsFileName,
            "--ion-imports-for-benchmark",
            importsV2FileName,
            "--ion-imports-for-benchmark",
            "none",
            "input1.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().importsForBenchmarkFile(importsFileName));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().importsForBenchmarkFile(importsV2FileName));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.importsForBenchmarkFile, optionsCombination.importsForBenchmarkFile));

            assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

    @Test
    public void writeBinaryFromDataWithImports() throws Exception {
        String importsFileName = fileInTestDirectory("imports.ion").toString();
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
            "binaryWithImports.10n"
        );
        ExpectedWriteOptionsCombination.defaultOptions()
            .importsForInputFile(importsFileName)
            .importsForBenchmarkFile(null)
            .ioType(IoType.BUFFER)
            .useSymbolTokens(true)
            .assertOptionsEqual(optionsCombination);

        assertWriteTaskExecutesCorrectly("binaryWithImports.10n", optionsCombination, Format.ION_BINARY, IoType.BUFFER);
        assertWriteTaskExecutesCorrectly("textWithImports.ion", optionsCombination, Format.ION_BINARY, IoType.BUFFER);
    }

    @Test
    public void importsForBenchmarkAuto() throws Exception {
        String importsFileName = fileInTestDirectory("imports.ion").toString();
        WriteOptionsCombination combination1 = parseSingleOptionsCombination(
            "write",
            "--ion-imports-for-input",
            importsFileName,
            "binaryWithImports.10n"
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
            "binaryWithImports.10n"
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
            "binaryWithImports.10n"
        );
        ExpectedReadOptionsCombination.defaultOptions()
            .importsForInputFile(importsFileName)
            .importsForBenchmarkFile(importsFileName)
            .assertOptionsEqual(combination3);

        ReadOptionsCombination combination4 = parseSingleOptionsCombination(
            "read",
            "--ion-imports-for-input",
            "none",
            "binaryWithImports.10n"
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
            "binaryWithImports.10n"
        );
        Path inputPath = fileInTestDirectory("binaryWithImports.10n");
        assertThrows(IllegalArgumentException.class, () -> optionsCombination.createMeasurableTask(inputPath));
    }

    @Test
    public void writeBinaryFromDataWithImportsWithoutProvidingCatalogRaisesError() throws Exception {
        WriteOptionsCombination optionsCombination = parseSingleOptionsCombination(
            "write",
            "binaryWithImports.10n"
        );
        Path inputPath = fileInTestDirectory("binaryWithImports.10n");
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
            "input1.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedReadOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().floatWidth(32));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions().floatWidth(64));
        expectedCombinations.add(ExpectedReadOptionsCombination.defaultOptions());

        for (ReadOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.floatWidth, optionsCombination.floatWidth));

            assertReadTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, optionsCombination.floatWidth != null);
            assertReadTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, true);
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
            "input1.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().floatWidth(32));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().floatWidth(64));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.floatWidth, optionsCombination.floatWidth));

            assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
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
            "input1.10n"
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
            "input1.10n"
        );
        assertEquals(3, optionsCombinations.size());
        List<ExpectedWriteOptionsCombination> expectedCombinations = new ArrayList<>(3);

        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ionWriterBlockSize(16));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions().ionWriterBlockSize(1024));
        expectedCombinations.add(ExpectedWriteOptionsCombination.defaultOptions());

        for (WriteOptionsCombination optionsCombination : optionsCombinations) {
            expectedCombinations.removeIf(expectedCandidate -> nullSafeEquals(expectedCandidate.ionWriterBlockSize, optionsCombination.ionWriterBlockSize));

            assertWriteTaskExecutesCorrectly("input1.10n", optionsCombination, Format.ION_BINARY, IoType.FILE);
            assertWriteTaskExecutesCorrectly("input1.ion", optionsCombination, Format.ION_BINARY, IoType.FILE);
        }
        assertTrue(expectedCombinations.isEmpty());
    }

}
