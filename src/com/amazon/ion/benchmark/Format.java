package com.amazon.ion.benchmark;

import com.amazon.ion.impl._Private_IonConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;

enum Format {
    ION_BINARY() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_BINARY:
                    if (
                        options.limit == Integer.MAX_VALUE
                        && options.flushPeriod == null
                        && options.preallocation == null
                    ) {
                        // There are no settings that require mutating the original input.
                        return input;
                    } else if (options.flushPeriod == null && options.preallocation == null) {
                        // This combination of settings requires simple truncation.
                        return IonUtilities.truncateBinaryIonFile(input, output, options.limit);
                    } else {
                        // This combination of settings requires re-encoding the input.
                        IonUtilities.rewriteIonFile(input, output, options, IonUtilities::newBinaryWriterSupplier);
                    }
                    break;
                case ION_TEXT:
                    IonUtilities.rewriteIonFile(input, output, options, IonUtilities::newBinaryWriterSupplier);
                    break;
            }
            return output;
        }

        @Override
        String getSuffix() {
            return ".10n";
        }

        @Override
        MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
            return new IonMeasurableReadTask(inputPath, options);
        }

        @Override
        MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
            return new IonMeasurableWriteTask(inputPath, options);
        }
    },
    ION_TEXT() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_TEXT:
                    if (options.limit == Integer.MAX_VALUE) {
                        // The input is already text and it is not being limited.
                        return input;
                    }
                    IonUtilities.rewriteIonFile(input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
                case ION_BINARY:
                    IonUtilities.rewriteIonFile(input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
            }
            return output;
        }

        @Override
        String getSuffix() {
            return ".ion";
        }

        @Override
        MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
            return new IonMeasurableReadTask(inputPath, options);
        }

        @Override
        MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
            return new IonMeasurableWriteTask(inputPath, options);
        }
    };

    abstract Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException;

    abstract String getSuffix();

    abstract MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException;

    abstract MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException;

    private static Format classify(Path path) throws IOException {
        File file = path.toFile();
        // TODO use the length of the longest format header.
        byte[] firstBytes = new byte[_Private_IonConstants.BINARY_VERSION_MARKER_SIZE];
        try (InputStream inputStream = new FileInputStream(file)) {
            int bytesRead = inputStream.read(firstBytes);
            if (bytesRead == _Private_IonConstants.BINARY_VERSION_MARKER_SIZE) {
                if (Arrays.equals(_Private_IonConstants.BINARY_VERSION_MARKER_1_0, firstBytes)) {
                    return Format.ION_BINARY;
                }
            }
            // TODO compare against other formats that have self-identifying headers.
        }
        // No format headers matched. Fall back on file suffix.
        if (file.getName().endsWith(".ion")) {
            return Format.ION_TEXT;
        }
        throw new IllegalArgumentException("Unknown file format.");
    }
}
