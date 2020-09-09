package com.amazon.ion.benchmark;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    private static SymbolTable[] parseImportsFromFile(String importsFile) throws IOException {
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
        return _Private_IonManagedBinaryWriterBuilder.create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.POOLED)
            //.withUserBlockSize(builder.ionWriterBlockSize) // TODO?
            .withPaddedLengthPreallocation(options.preallocation != null ? options.preallocation : 2)
            .withImports(parseImportsFromFile(options.importsFile))
            .withLocalSymbolTableAppendEnabled()
            ::newWriter;
    }

    /**
     * Creates a new IonWriterSupplier of text IonWriter instances.
     * @param options the options to use when creating writers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonWriterSupplier newTextWriterSupplier(OptionsCombinationBase options) throws IOException {
        return IonTextWriterBuilder.standard().withImports(parseImportsFromFile(options.importsFile))::build;
    }

    /**
     * Create a new IonReaderBuilder with the given options.
     * @param options the options to use when creating readers.
     * @return a new instance.
     * @throws IOException if thrown when parsing shared symbol tables.
     */
    static IonReaderBuilder newReaderBuilder(OptionsCombinationBase options) throws IOException {
        SimpleCatalog catalog = null;
        SymbolTable[] sharedSymbolTables = parseImportsFromFile(options.importsFile);
        if (sharedSymbolTables != null) {
            catalog = new SimpleCatalog();
            for (SymbolTable sharedSymbolTable : sharedSymbolTables) {
                catalog.putTable(sharedSymbolTable);
            }
        }
        return IonReaderBuilder.standard().withCatalog(catalog);
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
            if (options.flushPeriod == null && options.format == Format.ION_BINARY) {
                // Use system-level reader to preserve the same symbol tables from the input.
                writer = writerSupplier.get(options.newOutputStream(outputFile));
                reader = ((_Private_IonSystem) ION_SYSTEM).newSystemReader(options.newInputStream(inputFile));
            } else {
                // Do not preserve the existing symbol table boundaries.
                writer = writerSupplier.get(options.newOutputStream(outputFile));
                reader = newReaderBuilder(options).build(options.newInputStream(inputFile));
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
}
