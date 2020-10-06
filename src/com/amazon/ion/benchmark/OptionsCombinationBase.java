package com.amazon.ion.benchmark;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.function.Function;

import static com.amazon.ion.benchmark.Constants.FLUSH_PERIOD_NAME;
import static com.amazon.ion.benchmark.Constants.FORMAT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_API_NAME;
import static com.amazon.ion.benchmark.Constants.ION_FLOAT_WIDTH_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_FOR_BENCHMARK_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_FOR_INPUT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_USE_SYMBOL_TOKENS_NAME;
import static com.amazon.ion.benchmark.Constants.IO_BUFFER_SIZE_NAME;
import static com.amazon.ion.benchmark.Constants.IO_TYPE_NAME;
import static com.amazon.ion.benchmark.Constants.LIMIT_NAME;
import static com.amazon.ion.benchmark.Constants.PREALLOCATION_NAME;

/**
 * Represents a combination of options to be used by a single benchmark trial.
 */
abstract class OptionsCombinationBase {

    final Integer preallocation;
    final Integer flushPeriod;
    final Format format;
    final IonAPI api;
    final IoType ioType;
    final Integer ioBufferSize;
    final String importsForInputFile;
    final String importsForBenchmarkFile;
    final Integer floatWidth;
    final boolean useSymbolTokens;
    final int limit;

    /**
     * Retrieves and translates a value from the struct, if the field is present and is not the 'auto' value. Otherwise,
     * returns the provided default value.
     * @param options the struct from which to extract the value.
     * @param fieldName the name of the field to retrieve.
     * @param translator the function used to translate a matching value from an IonValue to the required type.
     * @param defaultValue the value to return if the requested field is not present in the struct or if the value is
     *                     'auto'.
     * @param <T> the type of value to retrieve.
     * @return the translated value from the struct or the provided default value.
     */
    static <T> T getOrDefault(IonStruct options, String fieldName, Function<IonValue, T> translator, T defaultValue) {
        IonValue value = options.get(fieldName);
        if (
            value == null
            || (IonType.isText(value.getType()) && ((IonText) value).stringValue().equals(Constants.AUTO_VALUE))
        ) {
            return defaultValue;
        }
        return translator.apply(value);
    }

    /**
     * @param options Ion representation of the options.
     */
    OptionsCombinationBase(String options) {
        IonStruct parametersStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(options);
        preallocation = getOrDefault(parametersStruct, PREALLOCATION_NAME, val -> ((IonInt) val).intValue(), null);
        flushPeriod = getOrDefault(parametersStruct, FLUSH_PERIOD_NAME, val -> ((IonInt) val).intValue(), null);
        format = getOrDefault(parametersStruct, FORMAT_NAME, val -> Format.valueOf(((IonText) val).stringValue()), Format.ION_BINARY);
        api = getOrDefault(parametersStruct, ION_API_NAME, val -> IonAPI.valueOf(((IonText) val).stringValue()), IonAPI.STREAMING);
        ioType = getOrDefault(parametersStruct, IO_TYPE_NAME, val -> IoType.valueOf(((IonText) val).stringValue()), IoType.FILE);
        ioBufferSize = getOrDefault(parametersStruct, IO_BUFFER_SIZE_NAME, val -> ((IonInt) val).intValue(), null);
        importsForInputFile = getOrDefault(parametersStruct, ION_IMPORTS_FOR_INPUT_NAME, val -> ((IonText) val).stringValue(), null);
        importsForBenchmarkFile = getOrDefault(parametersStruct, ION_IMPORTS_FOR_BENCHMARK_NAME, val -> ((IonText) val).stringValue(), null);
        floatWidth = getOrDefault(parametersStruct, ION_FLOAT_WIDTH_NAME, val -> ((IonInt) val).intValue(), null);
        useSymbolTokens = getOrDefault(parametersStruct, ION_USE_SYMBOL_TOKENS_NAME, val -> ((IonBool) val).booleanValue(), false);
        limit = getOrDefault(parametersStruct, LIMIT_NAME, val -> ((IonInt) val).intValue(), Integer.MAX_VALUE);
    }

    /**
     * Convert the input file to match the options, if necessary.
     * @param inputFile the input file.
     * @return the Path to the resulting file, which will be the same as the input file if no conversion was required.
     * @throws IOException if thrown during conversion.
     */
    final Path convertFileIfNecessary(Path inputFile) throws IOException {
        return format.convert(
            inputFile,
            TemporaryFiles.newTempFile(inputFile.toFile().getName(), format.getSuffix()),
            this
        );
    }

    /**
     * Creates a measurable task for this options combination over the given input file, converting the input file
     * to match the options if necessary.
     * @param inputFile the name of the file containing the data to be processed by the task.
     * @return a new MeasurableTask instance.
     * @throws Exception if an error occurs while preparing the task.
     */
    protected abstract MeasurableTask createMeasurableTask(Path inputFile) throws Exception;

    /**
     * Creates a new OptionsCombinationBase from the given Ion representation of an options combination.
     * @param optionsIon the Ion representation of the options combination.
     * @return a new instance, which may be any concrete implementation of OptionsCombinationBase.
     * @throws IOException if thrown while parsing the options.
     */
    static OptionsCombinationBase from(String optionsIon) throws IOException {
        IonStruct parametersStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(optionsIon);
        String firstAnnotation = parametersStruct.getTypeAnnotations()[0];
        OptionsCombinationBase options;
        if (firstAnnotation.equals("read")) {
            options = new ReadOptionsCombination(optionsIon);
        } else if (firstAnnotation.equals("write")) {
            options = new WriteOptionsCombination(optionsIon);
        } else {
            throw new IllegalArgumentException("Malformed options: must be annotated with the command name.");
        }
        return options;
    }

    /**
     * Creates a new InputStream over the given file.
     * @param file the file to be read.
     * @return a new InputStream matching the options.
     * @throws IOException if thrown when constructing the InputStream.
     */
    InputStream newInputStream(File file) throws IOException {
        if (ioBufferSize == null) {
            return new BufferedInputStream(new FileInputStream(file));
        }
        return new BufferedInputStream(new FileInputStream(file), ioBufferSize);
    }

    /**
     * Creates a new OutputStream over the given file. If the file already exists, it will be overwritten.
     * @param file the file to be written.
     * @return a new OutputStream matching the options.
     * @throws IOException if thrown when constructing the OutputStream.
     */
    OutputStream newOutputStream(File file) throws IOException {
        if (ioBufferSize == null) {
            return new BufferedOutputStream(new FileOutputStream(file, false));
        }
        return new BufferedOutputStream(new FileOutputStream(file, false), ioBufferSize);
    }

    /**
     * Creates a new ByteArrayOutputStream with the initial buffer size equivalent to {@link #ioBufferSize}. If
     * ioBufferSize is null, the default ByteArrayOutputStream constructor is used.
     * @return a new ByteArrayOutputStream.
     */
    ByteArrayOutputStream newByteArrayOutputStream() {
        if (ioBufferSize == null) {
            return new ByteArrayOutputStream();
        }
        return new ByteArrayOutputStream(ioBufferSize);
    }

}
