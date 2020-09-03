package com.amazon.ion.benchmark;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Represents a single combination of read command options that corresponds to a single read benchmark trial.
 */
class WriteOptionsCombination extends OptionsCombinationBase {

    WriteOptionsCombination(String parameters) {
        super(parameters);
    }

    @Override
    protected MeasurableTask createMeasurableTask(Path convertedInput) throws IOException {
        return format.createWriteTask(convertedInput, this);
    }
}
