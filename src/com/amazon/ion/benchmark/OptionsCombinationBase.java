package com.amazon.ion.benchmark;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static com.amazon.ion.benchmark.Constants.FLUSH_PERIOD_NAME;
import static com.amazon.ion.benchmark.Constants.FORMAT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_API_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_NAME;
import static com.amazon.ion.benchmark.Constants.IO_TYPE_NAME;
import static com.amazon.ion.benchmark.Constants.LIMIT_NAME;
import static com.amazon.ion.benchmark.Constants.PREALLOCATION_NAME;

/**
 * Represents a single combination of options that corresponds to a single benchmark trial.
 */
abstract class OptionsCombinationBase {

    final Integer preallocation;
    final Integer flushPeriod;
    final Format format;
    final IonAPI api;
    final IoType ioType;
    final String importsFile;
    final int limit;

    static <T> T getOrDefault(IonStruct parameters, String fieldName, Function<IonValue, T> translator, T defaultValue) {
        IonValue value = parameters.get(fieldName);
        if (
            value == null
            || (IonType.isText(value.getType()) && ((IonText) value).stringValue().equals(Constants.AUTO_VALUE))
        ) {
            return defaultValue;
        }
        return translator.apply(value);
    }

    OptionsCombinationBase(String parameters) {
        IonStruct parametersStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(parameters);
        preallocation = getOrDefault(parametersStruct, PREALLOCATION_NAME, val -> ((IonInt) val).intValue(), null);
        flushPeriod = getOrDefault(parametersStruct, FLUSH_PERIOD_NAME, val -> ((IonInt) val).intValue(), null);
        format = getOrDefault(parametersStruct, FORMAT_NAME, val -> Format.valueOf(((IonText) val).stringValue()), Format.ION_BINARY);
        api = getOrDefault(parametersStruct, ION_API_NAME, val -> IonAPI.valueOf(((IonText) val).stringValue()), IonAPI.STREAMING);
        ioType = getOrDefault(parametersStruct, IO_TYPE_NAME, val -> IoType.valueOf(((IonText) val).stringValue()), IoType.FILE);
        importsFile = getOrDefault(parametersStruct, ION_IMPORTS_NAME, val -> ((IonText) val).stringValue(), null);
        limit = getOrDefault(parametersStruct, LIMIT_NAME, val -> ((IonInt) val).intValue(), Integer.MAX_VALUE);
    }

    final MeasurableTask createMeasurableTask(String inputFile) throws Exception {
        Path originalInput = Paths.get(inputFile);
        Path convertedInput = format.convert(
            originalInput,
            TemporaryFiles.newTempFile(originalInput.toFile().getName(), format.getSuffix()),
            this
        );
        return createMeasurableTask(convertedInput);
    }

    protected abstract MeasurableTask createMeasurableTask(Path convertedInput) throws Exception;

    static OptionsCombinationBase from(String parameters) throws IOException {
        IonStruct parametersStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(parameters);
        String firstAnnotation = parametersStruct.getTypeAnnotations()[0];
        OptionsCombinationBase options;
        if (firstAnnotation.equals("read")) {
            options = new ReadOptionsCombination(parameters);
        } else if (firstAnnotation.equals("write")) {
            options = new WriteOptionsCombination(parameters);
        } else {
            throw new IllegalArgumentException("Malformed options: must be annotated with the command name.");
        }
        return options;
    }

    InputStream newInputStream(File file) throws IOException {
        // TODO configurable buffer size?
        return new BufferedInputStream(new FileInputStream(file));
    }

    OutputStream newOutputStream(File file) throws IOException {
        // TODO configurable buffer size?
        return new BufferedOutputStream(new FileOutputStream(file));
    }

}
