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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.amazon.ion.benchmark.Constants.FLUSH_PERIOD_NAME;
import static com.amazon.ion.benchmark.Constants.FORMAT_NAME;
import static com.amazon.ion.benchmark.Constants.ION_API_NAME;
import static com.amazon.ion.benchmark.Constants.ION_IMPORTS_NAME;
import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;
import static com.amazon.ion.benchmark.Constants.IO_TYPE_NAME;
import static com.amazon.ion.benchmark.Constants.LIMIT_NAME;
import static com.amazon.ion.benchmark.Constants.PREALLOCATION_NAME;

/**
 * Represents all options combinations, corresponding to all benchmark trials.
 */
abstract class OptionsCombinationsBase {

    private final String inputFile;
    private final String[] serializedParameters;
    private final boolean profile;
    private final Options options;

    static void addParameterTo(
        List<IonStruct> parametersStructs,
        String newParameterName,
        IonValue newParameter
    ) {
        for (IonStruct parametersStruct : parametersStructs) {
            parametersStruct.put(newParameterName, newParameter.clone());
        }
    }

    private static void combineParametersWith(
        List<IonStruct> parametersStructs,
        String newParameterName,
        List<IonValue> newParameters
    ) {
        List<IonStruct> additionalParametersStructs = new ArrayList<>();
        for (int i = 0; i < newParameters.size(); i++) {
            IonValue newParameter = newParameters.get(i);
            if (i < newParameters.size() - 1) {
                List<IonStruct> combinedParametersStructs = new ArrayList<>();
                for (IonStruct existingParametersStruct : parametersStructs) {
                    combinedParametersStructs.add(existingParametersStruct.clone());
                }
                addParameterTo(combinedParametersStructs, newParameterName, newParameter);
                additionalParametersStructs.addAll(combinedParametersStructs);
            } else {
                addParameterTo(parametersStructs, newParameterName, newParameter);
            }
        }
        parametersStructs.addAll(additionalParametersStructs);
    }

    static <T> void parseAndCombine(
        Object serializedParameter,
        String newParameterName,
        Function<String, T> parser,
        Function<T, IonValue> translator,
        List<IonStruct> parametersSink
    ) {
        Set<T> values = new HashSet<>();
        collectFromListOrString(serializedParameter, s -> values.add(parser.apply(s)));
        List<IonValue> newParameters = new ArrayList<>(values.size());
        for (T value : values) {
            if (value == null) {
                if (values.size() > 1) {
                    // There are values in addition to the 'auto' value, so it must be included literally.
                    newParameters.add(ION_SYSTEM.newSymbol(Constants.AUTO_VALUE));
                }
            } else {
                newParameters.add(translator.apply(value));
            }
        }
        combineParametersWith(parametersSink, newParameterName, newParameters);
    }

    private static String[] serializeParameters(List<IonStruct> parametersStructs) {
        String[] serializedParameters = new String[parametersStructs.size()];
        int i = 0;
        for (IonStruct parametersStruct : parametersStructs) {
            serializedParameters[i] = parametersStruct.toString();
            i++;
        }
        return serializedParameters;
    }

    static void requireFileToExist(String fileName) {
        if (!new File(fileName).isFile()) {
            throw new IllegalArgumentException("File " + fileName + " does not exist or is not a normal file.");
        }
    }

    static String getStringOrNull(Object stringOrNull) {
        if (stringOrNull == null) {
            return null;
        }
        return stringOrNull.toString();
    }

    private static void collectFromListOrString(Object listOrString, Consumer<String> consumer) {
        if (listOrString instanceof List) {
            for (String compressionString : ((List<String>) listOrString)) {
                consumer.accept(compressionString);
            }
        } else {
            consumer.accept(getStringOrNull(listOrString.toString()));
        }
    }

    static Integer getIntOrAuto(String intOrAuto) {
        if (intOrAuto == null || intOrAuto.equals(Constants.AUTO_VALUE)) {
            return null;
        }
        return Integer.parseInt(intOrAuto);
    }

    OptionsCombinationsBase(String commandName, Map<String, Object> optionsMap) {
        inputFile = getStringOrNull(optionsMap.get("<input_file>"));
        if (inputFile == null) {
            throw new IllegalArgumentException("Must provide an input file");
        }
        profile = optionsMap.get("--profile").equals(true);
        List<IonStruct> parametersStructs = new ArrayList<>();
        IonStruct initialParametersStruct = ION_SYSTEM.newEmptyStruct();
        initialParametersStruct.addTypeAnnotation(commandName);
        parametersStructs.add(initialParametersStruct);
        if (optionsMap.get("--limit") != null) {
            int limit = Integer.parseInt(optionsMap.get("--limit").toString());
            addParameterTo(parametersStructs, LIMIT_NAME, ION_SYSTEM.newInt(limit));
        }
        IoType ioType = IoType.valueOf(optionsMap.get("--io-type").toString().toUpperCase());
        addParameterTo(parametersStructs, IO_TYPE_NAME, ION_SYSTEM.newSymbol(ioType.name()));
        String importsFile = getStringOrNull(optionsMap.get("--ion-imports"));
        if (importsFile != null) {
            requireFileToExist(importsFile);
            addParameterTo(parametersStructs, ION_IMPORTS_NAME, ION_SYSTEM.newString(importsFile));
        }
        parseAndCombine(
            optionsMap.get("--ion-length-preallocation"),
            PREALLOCATION_NAME,
            OptionsCombinationsBase::getIntOrAuto,
            ION_SYSTEM::newInt,
            parametersStructs
        );
        parseAndCombine(
            optionsMap.get("--ion-flush-period"),
            FLUSH_PERIOD_NAME,
            OptionsCombinationsBase::getIntOrAuto,
            ION_SYSTEM::newInt,
            parametersStructs
        );
        parseAndCombine(
            optionsMap.get("--ion-api"),
            ION_API_NAME,
            (s) -> IonAPI.valueOf(s.toUpperCase()),
            (api) -> ION_SYSTEM.newSymbol(api.name()),
            parametersStructs
        );
        parseAndCombine(
            optionsMap.get("--format"),
            FORMAT_NAME,
            (s) -> Format.valueOf(s.toUpperCase()),
            (format) -> ION_SYSTEM.newSymbol(format.name()),
            parametersStructs
        );
        parseTaskSpecificOptions(optionsMap, parametersStructs);
        serializedParameters = serializeParameters(parametersStructs);
        if (profile) {
            if (parametersStructs.size() > 1) {
                throw new IllegalArgumentException("Options must only be specified once when --profile is used.");
            }
            options = null;
        } else {
            ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .include(Bench.class.getSimpleName())
                .param("input", inputFile)
                .param("parameters", serializedParameters)
                .mode(Mode.SingleShotTime) // TODO make configurable. Single shot is good for large files, but SampleTime is good for really small files.
                .measurementIterations(Integer.parseInt(optionsMap.get("--iterations").toString()))
                .warmupIterations(Integer.parseInt(optionsMap.get("--warmups").toString()))
                .forks(Integer.parseInt(optionsMap.get("--forks").toString()))
                .addProfiler(HeapProfiler.class)
                .addProfiler(GCProfiler.class)
                .addProfiler(SerializedSizeProfiler.class)
                .shouldDoGC(true)
                .timeUnit(TimeUnit.MILLISECONDS); // TODO make configurable. Milliseconds is good for most files, but really small ones need Microseconds.
            String resultsFile = getStringOrNull(optionsMap.get("--results-file"));
            if (resultsFile != null) {
                optionsBuilder.result(resultsFile);
            }
            ResultsFormat resultsFormat = ResultsFormat.valueOf(optionsMap.get("--results-format").toString().toUpperCase());
            switch (resultsFormat) {
                case ION:
                    optionsBuilder.resultFormat(ResultFormatType.JSON);
                    break;
                case JMH:
                    // This is the default format. If no results file is specified, do not explicitly specify the
                    // output format because this causes JMH to write the results to a file with a default name.
                    if (resultsFile != null) {
                        optionsBuilder.resultFormat(ResultFormatType.TEXT);
                    }
                    break;
            }
            options = optionsBuilder.build();
        }
    }

    abstract void parseTaskSpecificOptions(Map<String, Object> optionsMap, List<IonStruct> parametersSink);

    void executeBenchmark() throws Exception {
        TemporaryFiles.prepareTempDirectory();
        if (profile) {
            OptionsCombinationBase options = OptionsCombinationBase.from(serializedParameters[0]);
            MeasurableTask measurableTask = options.createMeasurableTask(inputFile);
            measurableTask.setUpTrial();
            Callable<Void> task = measurableTask.getTask();
            System.out.println("Entering profiling mode. Type q (followed by Enter/Return) to terminate after the next complete iteration.");
            while (System.in.available() <= 0 || System.in.read() != 'q') {
                measurableTask.setUpIteration();
                task.call();
                measurableTask.tearDownIteration();
            }
        } else {
            new Runner(options).run();
        }
        TemporaryFiles.cleanUpTempDirectory();
    }

    static OptionsCombinationsBase from(Map<String, Object> optionsMap) {
        OptionsCombinationsBase options;
        if (optionsMap.get("write").equals(true)) {
            options = new WriteOptionsCombinations(optionsMap);
        } else if (optionsMap.get("read").equals(true)) {
            options = new ReadOptionsCombinations(optionsMap);
        } else {
            throw new IllegalArgumentException("Unknown command. Select from: write, read.");
        }
        return options;
    }
}
