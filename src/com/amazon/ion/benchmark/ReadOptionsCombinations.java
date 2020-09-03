package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;

import java.util.List;
import java.util.Map;

import static com.amazon.ion.benchmark.Constants.ION_READER_NAME;
import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;
import static com.amazon.ion.benchmark.Constants.PATHS_NAME;

/**
 * Represents all read command options combinations, corresponding to all read benchmark trials.
 */
class ReadOptionsCombinations extends OptionsCombinationsBase {

    ReadOptionsCombinations(Map<String, Object> optionsMap) {
        super("read", optionsMap);
    }

    @Override
    void parseTaskSpecificOptions(Map<String, Object> optionsMap, List<IonStruct> parametersSink) {
        String pathsFile = getStringOrNull(optionsMap.get("--paths"));
        if (pathsFile != null) {
            requireFileToExist(pathsFile);
            addParameterTo(parametersSink, PATHS_NAME, ION_SYSTEM.newString(pathsFile));
        }
        parseAndCombine(
            optionsMap.get("--ion-reader"),
            ION_READER_NAME,
            (s) -> IonReaderType.valueOf(s.toUpperCase()),
            (type) -> ION_SYSTEM.newSymbol(type.name()),
            parametersSink
        );
    }

}
