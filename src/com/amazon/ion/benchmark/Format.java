package com.amazon.ion.benchmark;

import com.amazon.ion.impl._Private_IonConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.amazon.ion.benchmark.IonUtilities.getMinorVersion;
import static com.amazon.ion.benchmark.IonUtilities.isFormatHeaderPresent;

/**
 * Data formats, e.g. Ion binary and Ion text.
 */
enum Format {
    ION_BINARY() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_BINARY:
                    boolean optionsRequireRewrite = options.flushPeriod != null
                        || options.ionDelimitedContainers != null
                        || options.ionInlineSymbols != null
                        || options.preallocation != null
                        || options.floatWidth != null
                        || (options.ionMinorVersion != null && !IonUtilities.minorVersionsEqual(Format.ION_BINARY, options.ionMinorVersion, input.toFile()))
                        || (options.importsForBenchmarkFile != null
                            && !IonUtilities.importsEqual(options.importsForBenchmarkFile, input.toFile()))
                        || !IonUtilities.importsFilesEqual(options.importsForInputFile, options.importsForBenchmarkFile);
                    if (optionsRequireRewrite) {
                        // This combination of settings requires re-encoding the input.
                        IonUtilities.rewriteIonFile(ION_BINARY, input, output, options, IonUtilities::newBinaryWriterSupplier);
                    } else if (options.limit == Integer.MAX_VALUE) {
                        // There are no settings that require mutating the original input.
                        return input;
                    } else if (options.ionMinorVersion != null && options.ionMinorVersion > 0) {
                        // TODO this case is eligible for simple truncation (the 'else' below), but currently it does
                        //  not work because the SpanProvider facet, which is used by our truncation implementation,
                        //  can't handle macro invocations or delimited containers. If that changes, this branch will
                        //  no longer be necessary. See https://github.com/amazon-ion/ion-java/issues/1011
                        IonUtilities.rewriteIonFile(ION_BINARY, input, output, options, IonUtilities::newBinaryWriterSupplier);
                    } else {
                        // This combination of settings requires simple truncation.
                        return IonUtilities.truncateBinaryIonFile(input, output, options.limit);
                    }
                    break;
                case ION_TEXT:
                    IonUtilities.rewriteIonFile(ION_TEXT, input, output, options, IonUtilities::newBinaryWriterSupplier);
                    break;
                case JSON:
                    // TODO add an option to "upconvert" from JSON. For example, detect timestamps contained in
                    // JSON strings and write them as Ion timestamps.
                    IonUtilities.rewriteIonFile(JSON, input, output, options, IonUtilities::newBinaryWriterSupplier);
                    break;
                case CBOR:
                    // TODO add an option to "upconvert" from CBOR. For example, detect symbol values.
                    IonUtilities.rewriteCborToIon(input, output, options, IonUtilities::newBinaryWriterSupplier);
                    break;
            }
            return output;
        }

        @Override
        boolean canParse(Format otherFormat) {
            return otherFormat.isIon() || otherFormat == Format.JSON;
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
            return IonUtilities.createIonMeasurableWriteTask(inputPath, options);
        }

        @Override
        boolean isIon() {
            return true;
        }
    },
    ION_TEXT() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_TEXT:
                    if (
                        options.flushPeriod == null &&
                        options.limit == Integer.MAX_VALUE &&
                        (getMinorVersion(ION_TEXT, input.toFile()) == options.ionMinorVersion)
                    ) {
                        // The input is already text in the requested minor version, and it is not being limited or
                        // written with a specific flush period.
                        return input;
                    }
                    IonUtilities.rewriteIonFile(ION_TEXT, input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
                case ION_BINARY:
                    IonUtilities.rewriteIonFile(ION_BINARY, input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
                case JSON:
                    // TODO add an option to "upconvert" from JSON. For example, detect timestamps contained in
                    // JSON strings and write them as Ion timestamps.
                    IonUtilities.rewriteIonFile(JSON, input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
                case CBOR:
                    // TODO add an option to "upconvert" from CBOR. For example, detect symbol values.
                    IonUtilities.rewriteCborToIon(input, output, options, IonUtilities::newTextWriterSupplier);
                    break;
            }
            return output;
        }

        @Override
        boolean canParse(Format otherFormat) {
            return otherFormat.isIon() || otherFormat == Format.JSON;
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
            return IonUtilities.createIonMeasurableWriteTask(inputPath, options);
        }

        @Override
        boolean isIon() {
            return true;
        }
    },
    JSON() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_TEXT:
                case ION_BINARY:
                    // Down-convert to JSON.
                    IonUtilities.rewriteIonFile(sourceFormat, input, output, options, IonUtilities::newJsonWriterSupplier);
                    break;
                case JSON:
                    if (options.limit == Integer.MAX_VALUE) {
                        // The input is already JSON and it is not being limited.
                        return input;
                    }
                    JacksonUtilities.rewriteJsonFile(input, output, options);
                    break;
                case CBOR:
                    JacksonUtilities.rewriteCborToJson(input, output, options);
                    break;
            }
            return output;
        }

        @Override
        boolean canParse(Format otherFormat) {
            return otherFormat == Format.JSON;
        }

        @Override
        String getSuffix() {
            return ".json";
        }

        @Override
        MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
            return new JsonJacksonMeasurableReadTask(inputPath, options);
        }

        @Override
        MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
            return new JsonJacksonMeasurableWriteTask(inputPath, options);
        }

        @Override
        boolean isIon() {
            return false;
        }
    },
    CBOR() {
        @Override
        Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException {
            Format sourceFormat = classify(input);
            switch (sourceFormat) {
                case ION_BINARY:
                case ION_TEXT:
                    IonUtilities.rewriteIonFile(sourceFormat, input, output, options, IonUtilities::newCborWriterSupplier);
                    break;
                case JSON:
                    JacksonUtilities.rewriteJsonToCbor(input, output, options);
                    break;
                case CBOR:
                    if (options.limit == Integer.MAX_VALUE) {
                        // The input is already CBOR and it is not being limited.
                        return input;
                    }
                    JacksonUtilities.rewriteCborFile(input, output, options);
                    break;
            }
            return output;
        }

        @Override
        boolean canParse(Format otherFormat) {
            return otherFormat == Format.CBOR;
        }

        @Override
        String getSuffix() {
            return ".cbor";
        }

        @Override
        MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
            return new CborJacksonMeasurableReadTask(inputPath, options);
        }

        @Override
        MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException {
            return new CborJacksonMeasurableWriteTask(inputPath, options);
        }

        @Override
        boolean isIon() {
            return false;
        }
    };

    /**
     * Convert the input data to this format using the given options.
     * @param input the input data.
     * @param output the destination for the converted data.
     * @param options the options to use for the conversion.
     * @return the path to the converted data. This will be `input` if no conversion was required, or `output` if
     *   conversion was required.
     * @throws IOException if thrown during conversion.
     */
    abstract Path convert(Path input, Path output, OptionsCombinationBase options) throws IOException;

    /**
     * @param otherFormat a Format.
     * @return if data in the given format can be read natively by parsers of this Format.
     */
    abstract boolean canParse(Format otherFormat);

    /**
     * @return the file suffix associated with this format.
     */
    abstract String getSuffix();

    /**
     * Create a MeasurableReadTask with the given input and options for this format.
     * @param inputPath the data to be read.
     * @param options the options to use when reading.
     * @return a new MeasurableReadTask.
     * @throws IOException if thrown while handling options.
     */
    abstract MeasurableReadTask createReadTask(Path inputPath, ReadOptionsCombination options) throws IOException;

    /**
     * Create a MeasurableWriteTask with the given input and options for this format.
     * @param inputPath the data to be written.
     * @param options the options to use when writing.
     * @return a new MeasurableWriteTask.
     * @throws IOException if thrown while handling options.
     */
    abstract MeasurableWriteTask createWriteTask(Path inputPath, WriteOptionsCombination options) throws IOException;

    /**
     * @return true if the format is an Ion (text or binary).
     */
    abstract boolean isIon();

    /**
     * Determine which Format the data at the given path represents.
     * @param path the path to the data to be classified.
     * @return the Format of the data.
     * @throws IOException if thrown while reading the data.
     * @throws IllegalArgumentException if the data does not match a known Format.
     */
    static Format classify(Path path) throws IOException {
        File file = path.toFile();
        if (isFormatHeaderPresent(_Private_IonConstants.BINARY_VERSION_MARKER_1_0, file)
            || isFormatHeaderPresent(_Private_IonConstants.BINARY_VERSION_MARKER_1_1, file)) {
            return Format.ION_BINARY;
        }
        if (isFormatHeaderPresent(CborUtilities.CBOR_SELF_IDENTIFICATION_TAG, file)) {
            return Format.CBOR;
        }
        // No format headers matched. Fall back on file suffix.
        if (file.getName().endsWith(Format.ION_TEXT.getSuffix())) {
            return Format.ION_TEXT;
        }
        if (file.getName().endsWith(Format.JSON.getSuffix())) {
            return Format.JSON;
        }
        throw new IllegalArgumentException("Unknown file format.");
    }
}
