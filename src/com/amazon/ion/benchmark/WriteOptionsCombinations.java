package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Represents all write command options combinations, corresponding to all write benchmark trials.
 */
class WriteOptionsCombinations extends OptionsCombinationsBase {

    WriteOptionsCombinations(Map<String, Object> optionsMap) {
        super("write", optionsMap);
    }

    @Override
    void parseTaskSpecificOptions(Map<String, Object> optionsMap, List<IonStruct> parametersSink) {
        // There are currently no write-specific options.
    }

}
