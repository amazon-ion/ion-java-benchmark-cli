package com.amazon.ion.benchmark;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;

/**
 * Utility class for Ion-related functions.
 */
class IonUtilities {

    private IonUtilities() {
        // Do not instantiate.
    }

    /**
     * Parse shared symbol tables from the given file.
     * @param importsFile the name of the file containing shared symbol tables.
     * @return an array of shared symbol tables.
     * @throws IOException if thrown while reading the file.
     */
    static SymbolTable[] parseImportsFromFile(String importsFile) throws IOException {
        if (importsFile == null) {
            return null;
        }
        List<SymbolTable> sharedSymbolTables = new ArrayList<>();
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(importsFile)))) {
            while (reader.next() != null) {
                if (reader.getType() != IonType.STRUCT) {
                    continue;
                }
                sharedSymbolTables.add(Constants.ION_SYSTEM.newSharedSymbolTable(reader, true));
            }
        }
        return sharedSymbolTables.toArray(new SymbolTable[]{});
    }

    /**
     * Determines whether shared SymbolTable sequences are equivalent.
     * @param lhsImports a sequence of shared SymbolTables.
     * @param rhsImports another sequence of shared SymbolTables.
     * @return true if both sequences are equivalent; otherwise, false.
     */
    static boolean importsEqual(SymbolTable[] lhsImports, SymbolTable[] rhsImports) {
        if ((lhsImports == null || lhsImports.length == 0) != (rhsImports == null || rhsImports.length == 0)) {
            return false;
        }
        if (lhsImports == null || lhsImports.length == 0) {
            return true;
        }
        if (lhsImports.length != rhsImports.length) {
            return false;
        }
        for (int i = 0; i < rhsImports.length; i++) {
            SymbolTable lhsImport = rhsImports[i];
            SymbolTable rhsImport = lhsImports[i];
            if (!lhsImport.getName().equals(rhsImport.getName())) {
                return false;
            }
            if (lhsImport.getVersion() != rhsImport.getVersion()) {
                return false;
            }
            if (lhsImport.getMaxId() != rhsImport.getMaxId()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the Ion data provided by 'input' contains the shared symbol table imports included in
     * 'importsFile'.
     * @param importsFile a file containing shared symbol tables.
     * @param input a stream of Ion data.
     * @return true if the file and the stream contain the same shared symbol table imports; otherwise, false.
     * @throws IOException if thrown when reading from the stream or file.
     */
    static boolean importsEqual(String importsFile, InputStream input) throws IOException {
        try (IonReader reader = IonReaderBuilder.standard().build(input)) {
            reader.next();
            SymbolTable[] lhsImports = reader.getSymbolTable().getImportedTables();
            SymbolTable[] rhsImports = parseImportsFromFile(importsFile);
            return importsEqual(lhsImports, rhsImports);
        }
    }

    /**
     * See {@link #importsEqual(String, InputStream)}.
     * @param importsFile a file containing shared symbol tables.
     * @param input a file containing Ion data.
     * @return true if the file and the stream contain the same shared symbol table imports; otherwise, false.
     * @throws IOException if thrown when reading from the stream or file.
     */
    static boolean importsEqual(String importsFile, File input) throws IOException {
        try (InputStream inputStream = new FileInputStream(input)) {
            return importsEqual(importsFile, inputStream);
        }
    }

    /**
     * Determines whether the shared symbol tables included in both files are the same.
     * @param lhsImportsFile a file containing a sequence of shared symbol tables.
     * @param rhsImportsFile another file containing a sequence of shared symbol tables.
     * @return true if the files contain the same sequence of shared symbol tables; otherwise, false.
     * @throws IOException if thrown when reading from either file.
     */
    static boolean importsFilesEqual(String lhsImportsFile, String rhsImportsFile) throws IOException {
        SymbolTable[] lhsImports = parseImportsFromFile(lhsImportsFile);
        SymbolTable[] rhsImports = parseImportsFromFile(rhsImportsFile);
        return importsEqual(lhsImports, rhsImports);
    }

    /**
     * Supplies IonWriter instances.
     */
    @FunctionalInterface
    interface IonWriterSupplier {
        IonWriter get(OutputStream out) throws IOException;
    }

    /**
     * Supplies IonWriterSupplier instances for the given options.
     */
    @FunctionalInterface
    interface IonWriterSupplierFactory {
        IonWriterSupplier get(OptionsCombinationBase options) throws IOException;
    }

    /**
     * Creates a new IonWriterSupplier of binary IonWriter instances.
     * @param options the options to use when creating writers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonWriterSupplier newBinaryWriterSupplier(OptionsCombinationBase options) throws IOException {
        _Private_IonManagedBinaryWriterBuilder builder = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.POOLED)
            .withPaddedLengthPreallocation(options.preallocation != null ? options.preallocation : 2)
            .withImports(parseImportsFromFile(options.importsForBenchmarkFile))
            .withLocalSymbolTableAppendEnabled();
        if (options.floatWidth != null && options.floatWidth == 32) {
            builder.withFloatBinary32Enabled();
        } else {
            builder.withFloatBinary32Disabled();
        }
        if (options instanceof WriteOptionsCombination) {
            // When this method is used by the read benchmark for converting the input file, 'options' will be a
            // ReadOptionsCombination, which does not have the 'ionWriterUserBufferSize' value, because this value
            // does not affect the serialized Ion.
            Integer ionWriterBlockSize = ((WriteOptionsCombination) options).ionWriterBlockSize;
            if (ionWriterBlockSize != null) {
                builder.withUserBlockSize(ionWriterBlockSize);
            }
        }
        return builder::newWriter;
    }

    /**
     * Creates a new IonWriterSupplier of text IonWriter instances.
     * @param options the options to use when creating writers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonWriterSupplier newTextWriterSupplier(OptionsCombinationBase options) throws IOException {
        return IonTextWriterBuilder.standard().withImports(parseImportsFromFile(options.importsForBenchmarkFile))::build;
    }

    /**
     * Create a new IonCatalog populated with SharedSymbolTables read from the given file.
     * @param importsFile the file containing the shared symbol tables to import.
     * @return a new IonCatalog, or null if the given file is null.
     * @throws IOException if thrown while parsing the shared symbol tables from the file.
     */
    static IonCatalog newCatalog(String importsFile) throws IOException {
        SimpleCatalog catalog = null;
        SymbolTable[] sharedSymbolTables = parseImportsFromFile(importsFile);
        if (sharedSymbolTables != null) {
            catalog = new SimpleCatalog();
            for (SymbolTable sharedSymbolTable : sharedSymbolTables) {
                catalog.putTable(sharedSymbolTable);
            }
        }
        return catalog;
    }

    /**
     * Create a new IonReaderBuilder with the given options for use during read benchmarks.
     * @param options the options to use when creating readers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonReaderBuilder newReaderBuilderForBenchmark(OptionsCombinationBase options) throws IOException {
        return IonReaderBuilder.standard().withCatalog(newCatalog(options.importsForBenchmarkFile));
    }

    /**
     * Create a new IonReaderBuilder with the given options for use when reading the input file.
     * @param options the options to use when creating readers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonReaderBuilder newReaderBuilderForInput(OptionsCombinationBase options) throws IOException {
        return IonReaderBuilder.standard().withCatalog(newCatalog(options.importsForInputFile));
    }

    /**
     * Truncate the given binary Ion file to the given number of top-level values.
     * @param input a binary Ion file.
     * @param output the destination of the truncated file.
     * @param numberOfValues the maximum number of top-level values that the destination file will contain.
     * @return `output` if `input` required truncation; otherwise, `input`.
     * @throws IOException if thrown when reading or writing.
     */
    static Path truncateBinaryIonFile(Path input, Path output, int numberOfValues) throws IOException {
        File inputFile = input.toFile();
        long length = 0;
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(inputFile)))) {
            SpanProvider spanProvider = reader.asFacet(SpanProvider.class);
            for (int i = 0; i < numberOfValues; i++) {
                if (reader.next() == null) {
                    break;
                }
                length = spanProvider.currentSpan().asFacet(OffsetSpan.class).getFinishOffset();
            }
        }
        if (length < inputFile.length()) {
            Files.copy(input, output, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(output, StandardOpenOption.WRITE)) {
                channel.truncate(length);
            }
            return output;
        }
        return input;
    }

    /**
     * Rewrite values using the given options.
     * @param reader reader over the input data.
     * @param writer writer to the output data.
     * @param options the options to use when re-writing.
     * @throws IOException if thrown when reading or writing.
     */
    private static void writeValuesWithOptions(IonReader reader, IonWriter writer, OptionsCombinationBase options) throws IOException {
        int i = 0;
        boolean isUnlimited = options.limit == Integer.MAX_VALUE;
        while (isUnlimited || i < options.limit) {
            if (reader.next() == null) {
                break;
            }
            writer.writeValue(reader);
            if (options.flushPeriod != null && i % options.flushPeriod == 0) {
                writer.flush();
            }
            i++;
        }
    }

    /**
     * Rewrite the given Ion file using the given options.
     * @param input path to the file to re-write.
     * @param output path to the destination file.
     * @param options the options to use when rewriting.
     * @param writerSupplierFactory IonWriterSupplierFactory for retrieving suppliers of IonWriters with the given options.
     * @throws IOException if thrown when reading or writing.
     */
    static void rewriteIonFile(Path input, Path output, OptionsCombinationBase options, IonWriterSupplierFactory writerSupplierFactory) throws IOException {
        File inputFile = input.toFile();
        File outputFile = output.toFile();
        IonUtilities.IonWriterSupplier writerSupplier = writerSupplierFactory.get(options);
        IonWriter writer = null;
        IonReader reader = null;
        try {
            if (
                options.flushPeriod == null &&
                options.importsForInputFile == null &&
                options.importsForBenchmarkFile == null &&
                options.format == Format.ION_BINARY
            ) {
                // Use system-level reader to preserve the same symbol tables from the input.
                writer = writerSupplier.get(options.newOutputStream(outputFile));
                reader = ((_Private_IonSystem) ION_SYSTEM).newSystemReader(options.newInputStream(inputFile));
            } else {
                // Do not preserve the existing symbol table boundaries.
                writer = writerSupplier.get(options.newOutputStream(outputFile));
                reader = newReaderBuilderForInput(options).build(options.newInputStream(inputFile));
            }
            writeValuesWithOptions(reader, writer, options);
        } finally {
            if (writer != null) {
                writer.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Return an IonSystem compatible with the given options. If the options include shared symbol table imports
     * for use in benchmarks, the returned system will include and IonCatalog containing those imports.
     * @param options the options from which to construct the IonSystem.
     * @return an IonSystem.
     * @throws IOException if thrown when parsing imports.
     */
    static IonSystem ionSystemForBenchmark(OptionsCombinationBase options) throws IOException {
        if (options.importsForBenchmarkFile == null) {
            return ION_SYSTEM;
        }
        return IonSystemBuilder.standard().withCatalog(newCatalog(options.importsForBenchmarkFile)).build();
    }

    /**
     * Return an IonSystem compatible with the given options. If the options include shared symbol table imports
     * for use in with the input file, the returned system will include and IonCatalog containing those imports.
     * @param options the options from which to construct the IonSystem.
     * @return an IonSystem.
     * @throws IOException if thrown when parsing imports.
     */
    static IonSystem ionSystemForInput(OptionsCombinationBase options) throws IOException {
        if (options.importsForInputFile == null) {
            return ION_SYSTEM;
        }
        return IonSystemBuilder.standard().withCatalog(newCatalog(options.importsForInputFile)).build();
    }
}
