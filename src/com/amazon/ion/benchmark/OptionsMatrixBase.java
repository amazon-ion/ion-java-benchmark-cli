package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.amazon.ion.benchmark.Constants.FLUSH_PERIOD_NAME;
import static com.amazon.ion.benchmark.Constants.FORMAT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_API_NAME;
import static com.amazon.ion.benchmark.Constants.ION_FLOAT_WIDTH_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_FOR_BENCHMARK_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_FOR_INPUT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;
import static com.amazon.ion.benchmark.Constants.ION_USE_SYMBOL_TOKENS_NAME;
import static com.amazon.ion.benchmark.Constants.IO_TYPE_NAME;
import static com.amazon.ion.benchmark.Constants.LIMIT_NAME;
import static com.amazon.ion.benchmark.Constants.PREALLOCATION_NAME;

/**
 * Represents the matrix of all options combinations, corresponding to all benchmark trials.
 */
abstract class OptionsMatrixBase {

    private final String inputFile;
    private final String[] serializedOptionsCombinations;
    private final boolean profile;
    private final Options jmhOptions;

    /**
     * Add the given option to all existing combinations.
     * @param optionsCombinationStructs structs representing all existing options combinations.
     * @param newOptionName the name of the new option.
     * @param newOption the value of the new option.
     */
    static void addOptionTo(
        List<IonStruct> optionsCombinationStructs,
        String newOptionName,
        IonValue newOption
    ) {
        for (IonStruct optionsCombinationStruct : optionsCombinationStructs) {
            optionsCombinationStruct.put(newOptionName, newOption.clone());
        }
    }

    /**
     * Combines the given option with all existing combinations. This multiplies the number of combinations by the
     * size of the `newOptions` list.
     * @param optionsCombinationStructs structs representing all existing options combinations.
     * @param newOptionName the name of the new option.
     * @param newOptions the values of the new option.
     */
    private static void combineOptionsWith(
        List<IonStruct> optionsCombinationStructs,
        String newOptionName,
        List<IonValue> newOptions
    ) {
        List<IonStruct> additionalOptionsStructs = new ArrayList<>();
        for (int i = 0; i < newOptions.size(); i++) {
            IonValue newOption = newOptions.get(i);
            if (i < newOptions.size() - 1) {
                List<IonStruct> combinedOptionsStructs = new ArrayList<>();
                for (IonStruct existingOptionsStruct : optionsCombinationStructs) {
                    combinedOptionsStructs.add(existingOptionsStruct.clone());
                }
                addOptionTo(combinedOptionsStructs, newOptionName, newOption);
                additionalOptionsStructs.addAll(combinedOptionsStructs);
            } else {
                addOptionTo(optionsCombinationStructs, newOptionName, newOption);
            }
        }
        optionsCombinationStructs.addAll(additionalOptionsStructs);
    }

    /**
     * Throws an exception if invoked. Intended to be used as the `implicitDefault` argument to
     * {@link #parseAndCombine(Object, String, Function, Function, List, Supplier)} when the option to be parsed
     * must always have an explicit value.
     * @return never returns cleanly.
     */
    static IonValue noImplicitDefault() {
        throw new IllegalStateException("This option must always be specified explicitly.");
    }

    /**
     * Parses option(s) from the given List of Strings and combines the new options with all existing combinations. This
     * multiplies the number of combinations by the number of new options parsed.
     * @param optionValueList List of Strings containing option values.
     * @param newOptionName the name of the new option.
     * @param parser parser function from String to the desired type.
     * @param translator translator from the desired type to IonValue.
     * @param optionsCombinationStructs structs representing all existing options combinations.
     * @param implicitDefault supplier of the value that is implied by a `null` value being returned from the parser
     *                        function. This supplier is used when the implicit value is one of multiple values for
     *                        the option, and therefore must be declared explicitly.
     * @param <T> the type of the option.
     */
    static <T> void parseAndCombine(
        Object optionValueList,
        String newOptionName,
        Function<String, T> parser,
        Function<T, IonValue> translator,
        List<IonStruct> optionsCombinationStructs,
        Supplier<IonValue> implicitDefault
    ) {
        Set<T> values = new HashSet<>();
        for (String optionValue : ((List<String>) optionValueList)) {
            values.add(parser.apply(optionValue));
        }
        List<IonValue> newOptions = new ArrayList<>(values.size());
        for (T value : values) {
            if (value == null) {
                if (values.size() > 1) {
                    // There are values in addition to the implicit value, so it must be included literally.
                    newOptions.add(implicitDefault.get());
                }
            } else {
                newOptions.add(translator.apply(value));
            }
        }
        combineOptionsWith(optionsCombinationStructs, newOptionName, newOptions);
    }

    /**
     * Serialize the options combinations to text Ion.
     * @param optionsCombinationStructs the options combinations to serialize.
     * @return the serialized options combinations.
     */
    private static String[] serializeOptionsCombinations(List<IonStruct> optionsCombinationStructs) {
        String[] serializedOptionsCombinations = new String[optionsCombinationStructs.size()];
        int i = 0;
        for (IonStruct optionsCombinationStruct : optionsCombinationStructs) {
            serializedOptionsCombinations[i] = optionsCombinationStruct.toString();
            i++;
        }
        return serializedOptionsCombinations;
    }

    /**
     * @param fileName a file that must exist on disk.
     * @return fileName, if it exists.
     * @throws IllegalArgumentException if the file does not exist, or is not a normal file.
     */
    static String requireFileToExist(String fileName) {
        if (!new File(fileName).isFile()) {
            throw new IllegalArgumentException("File " + fileName + " does not exist or is not a normal file.");
        }
        return fileName;
    }

    /**
     * @param stringOrNull an object from which to retrieve a String representation, if possible.
     * @return the toString() representation of the given object, if non-null; otherwise, null.
     */
    static String getStringOrNull(Object stringOrNull) {
        if (stringOrNull == null) {
            return null;
        }
        return stringOrNull.toString();
    }

    /**
     * @param intOrAuto a String representation of an integer, or the String 'auto', or null.
     * @return null if the input is null or is 'auto'; otherwise, the integer parsed from the input.
     */
    static Integer getIntOrAuto(String intOrAuto) {
        if (intOrAuto == null || intOrAuto.equals(Constants.AUTO_VALUE)) {
            return null;
        }
        return Integer.parseInt(intOrAuto);
    }

    /**
     * @param fileNameOrNone a String filename, or the String 'none', or null.
     * @return null if the input is null or is 'none'; otherwise, the filename.
     * @throws IllegalArgumentException if the filename is not null/'none' and does not exist.
     */
    private static String getFileOrNone(String fileNameOrNone) {
        if (fileNameOrNone == null || fileNameOrNone.equals(Constants.NONE_VALUE)) {
            return null;
        }
        return requireFileToExist(fileNameOrNone);
    }

    /**
     * @param fileNameOrNone a String filename, or the String 'none' or 'auto', or null
     * @param defaultFile the default value to use if fileNameOrNone is 'auto'.
     * @return null if the input is null or is 'none', or if the input is 'auto' and defaultFile is 'none'; otherwise,
     *   the filename.
     * @throws IllegalArgumentException if the resolved filename does not exist.
     */
    private static String getFileOrDefault(String fileNameOrNone, String defaultFile) {
        if (fileNameOrNone == null || fileNameOrNone.equals(Constants.NONE_VALUE)) {
            return null;
        }
        if (fileNameOrNone.equals(Constants.AUTO_VALUE)) {
            return getFileOrNone(defaultFile);
        }
        return requireFileToExist(fileNameOrNone);
    }

    /**
     * @param commandName the name of the top-level command.
     * @param optionsMatrix Map representing the options matrix for this command. The values of the map are either
     *                      scalar values or Lists of scalar values.
     */
    OptionsMatrixBase(String commandName, Map<String, Object> optionsMatrix) {
        inputFile = getStringOrNull(optionsMatrix.get("<input_file>"));
        if (inputFile == null) {
            throw new IllegalArgumentException("Must provide an input file");
        }
        profile = optionsMatrix.get("--profile").equals(true);
        List<IonStruct> optionsCombinationStructs = new ArrayList<>();
        IonStruct initialOptionsStruct = ION_SYSTEM.newEmptyStruct();
        initialOptionsStruct.addTypeAnnotation(commandName);
        optionsCombinationStructs.add(initialOptionsStruct);
        if (optionsMatrix.get("--limit") != null) {
            int limit = Integer.parseInt(optionsMatrix.get("--limit").toString());
            addOptionTo(optionsCombinationStructs, LIMIT_NAME, ION_SYSTEM.newInt(limit));
        }
        IoType ioType = IoType.valueOf(optionsMatrix.get("--io-type").toString().toUpperCase());
        addOptionTo(optionsCombinationStructs, IO_TYPE_NAME, ION_SYSTEM.newSymbol(ioType.name()));
        String importsForInput = getFileOrNone(optionsMatrix.get("--ion-imports-for-input").toString());
        if (importsForInput != null) {
            addOptionTo(optionsCombinationStructs, ION_IMPORTS_FOR_INPUT_NAME, ION_SYSTEM.newString(importsForInput));
        }
        parseAndCombine(
            optionsMatrix.get("--ion-imports-for-benchmark"),
            ION_IMPORTS_FOR_BENCHMARK_NAME,
            (s) -> OptionsMatrixBase.getFileOrDefault(s, importsForInput),
            ION_SYSTEM::newString,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newSymbol(Constants.AUTO_VALUE)
        );
        parseAndCombine(
            optionsMatrix.get("--ion-length-preallocation"),
            PREALLOCATION_NAME,
            OptionsMatrixBase::getIntOrAuto,
            ION_SYSTEM::newInt,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newSymbol(Constants.AUTO_VALUE)
        );
        parseAndCombine(
            optionsMatrix.get("--ion-flush-period"),
            FLUSH_PERIOD_NAME,
            OptionsMatrixBase::getIntOrAuto,
            ION_SYSTEM::newInt,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newSymbol(Constants.AUTO_VALUE)
        );
        parseAndCombine(
            optionsMatrix.get("--ion-api"),
            ION_API_NAME,
            (s) -> IonAPI.valueOf(s.toUpperCase()),
            (api) -> ION_SYSTEM.newSymbol(api.name()),
            optionsCombinationStructs,
            OptionsMatrixBase::noImplicitDefault
        );
        parseAndCombine(
            optionsMatrix.get("--ion-use-symbol-tokens"),
            ION_USE_SYMBOL_TOKENS_NAME,
            (s) -> {
                boolean value = Boolean.parseBoolean(s.toLowerCase());
                if (value) {
                    return true;
                }
                return null;
            },
            ION_SYSTEM::newBool,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newBool(false)
        );
        parseAndCombine(
            optionsMatrix.get("--format"),
            FORMAT_NAME,
            (s) -> Format.valueOf(s.toUpperCase()),
            (format) -> ION_SYSTEM.newSymbol(format.name()),
            optionsCombinationStructs,
            OptionsMatrixBase::noImplicitDefault
        );
        parseAndCombine(
            optionsMatrix.get("--ion-float-width"),
            ION_FLOAT_WIDTH_NAME,
            OptionsMatrixBase::getIntOrAuto,
            (width) -> {
                if (width != 32 && width != 64) {
                    throw new IllegalArgumentException("--ion-float-width must be 32, 64, or auto.");
                }
                return ION_SYSTEM.newInt(width);
            },
            optionsCombinationStructs,
            () -> ION_SYSTEM.newSymbol(Constants.AUTO_VALUE)
        );
        parseCommandSpecificOptions(optionsMatrix, optionsCombinationStructs);
        serializedOptionsCombinations = serializeOptionsCombinations(optionsCombinationStructs);
        if (profile) {
            if (optionsCombinationStructs.size() > 1) {
                throw new IllegalArgumentException("Options must only be specified once when --profile is used.");
            }
            jmhOptions = null;
        } else {
            ChainedOptionsBuilder jmhOptionsBuilder = new OptionsBuilder()
                .include(Bench.class.getSimpleName())
                .param("input", inputFile)
                .param("options", serializedOptionsCombinations)
                .mode(Mode.valueOf(optionsMatrix.get("--mode").toString()))
                .measurementIterations(Integer.parseInt(optionsMatrix.get("--iterations").toString()))
                .warmupIterations(Integer.parseInt(optionsMatrix.get("--warmups").toString()))
                .forks(Integer.parseInt(optionsMatrix.get("--forks").toString()))
                .addProfiler(HeapProfiler.class)
                .addProfiler(GCProfiler.class)
                .addProfiler(SerializedSizeProfiler.class)
                .shouldDoGC(true)
                .timeUnit(TimeUnit.valueOf(optionsMatrix.get("--time-unit").toString().toUpperCase()));
            String resultsFile = getStringOrNull(optionsMatrix.get("--results-file"));
            if (resultsFile != null) {
                jmhOptionsBuilder.result(resultsFile);
            }
            ResultsFormat resultsFormat = ResultsFormat.valueOf(optionsMatrix.get("--results-format").toString().toUpperCase());
            switch (resultsFormat) {
                case ION:
                    jmhOptionsBuilder.resultFormat(ResultFormatType.JSON);
                    break;
                case JMH:
                    // This is the default format. If no results file is specified, do not explicitly specify the
                    // output format because this causes JMH to write the results to a file with a default name.
                    if (resultsFile != null) {
                        jmhOptionsBuilder.resultFormat(ResultFormatType.TEXT);
                    }
                    break;
            }
            jmhOptions = jmhOptionsBuilder.build();
        }
    }

    /**
     * @return the serialized options combinations generated by this matrix.
     */
    String[] getSerializedOptionsCombinations() {
        return serializedOptionsCombinations;
    }

    /**
     * Parse any options related to a specific command.
     * @param optionsMatrix Map representing the options matrix for this command. The values of the map are either
     *                      scalar values or Lists of scalar values.
     * @param optionsCombinationStructs structs representing all existing options combinations.
     */
    abstract void parseCommandSpecificOptions(Map<String, Object> optionsMatrix, List<IonStruct> optionsCombinationStructs);

    /**
     * Execute all trials (one trial per options combination) for this matrix. If `profile` mode is enabled, execute
     * the only trial until the user chooses to exit.
     * @throws Exception if an error occurs while setting up, executing, or cleaning up a trial.
     */
    void executeBenchmark() throws Exception {
        TemporaryFiles.prepareTempDirectory();
        if (profile) {
            OptionsCombinationBase options = OptionsCombinationBase.from(serializedOptionsCombinations[0]);
            MeasurableTask measurableTask = options.createMeasurableTask(Paths.get(inputFile));
            measurableTask.setUpTrial();
            Callable<Void> task = measurableTask.getTask();
            System.out.println("Entering profiling mode. Type q (followed by Enter/Return) to terminate after the next complete iteration.");
            while (System.in.available() <= 0 || System.in.read() != 'q') {
                measurableTask.setUpIteration();
                task.call();
                measurableTask.tearDownIteration();
            }
            measurableTask.tearDownTrial();
        } else {
            new Runner(jmhOptions).run();
        }
        TemporaryFiles.cleanUpTempDirectory();
    }

    /**
     * Creates a new OptionsMatrixBase from the given map representing an options matrix.
     * @param optionsMatrix Map representing the options matrix. The values of the map are either scalar values or Lists
     *                      of scalar values.
     * @return a new instance, which may be any concrete implementation of OptionsMatrixBase.
     */
    static OptionsMatrixBase from(Map<String, Object> optionsMatrix) {
        OptionsMatrixBase matrix;
        if (optionsMatrix.get("write").equals(true)) {
            matrix = new WriteOptionsMatrix(optionsMatrix);
        } else if (optionsMatrix.get("read").equals(true)) {
            matrix = new ReadOptionsMatrix(optionsMatrix);
        } else {
            throw new IllegalArgumentException("Unknown command. Select from: write, read.");
        }
        return matrix;
    }
}
