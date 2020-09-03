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

class IonUtilities {

    private IonUtilities() {
        // Do not instantiate.
    }

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

    @FunctionalInterface
    interface IonWriterSupplier {
        IonWriter get(OutputStream out) throws IOException;
    }

    @FunctionalInterface
    interface IonWriterSupplierFactory {
        IonWriterSupplier get(OptionsCombinationBase options) throws IOException;
    }

    static IonWriterSupplier newBinaryWriterSupplier(OptionsCombinationBase options) throws IOException {
        return _Private_IonManagedBinaryWriterBuilder.create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.POOLED)
            //.withUserBlockSize(builder.ionWriterBlockSize) // TODO?
            .withPaddedLengthPreallocation(options.preallocation != null ? options.preallocation : 2)
            .withImports(parseImportsFromFile(options.importsFile))
            .withLocalSymbolTableAppendEnabled()
            ::newWriter;
    }

    static IonWriterSupplier newTextWriterSupplier(OptionsCombinationBase options) throws IOException {
        return IonTextWriterBuilder.standard().withImports(parseImportsFromFile(options.importsFile))::build;
    }

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
