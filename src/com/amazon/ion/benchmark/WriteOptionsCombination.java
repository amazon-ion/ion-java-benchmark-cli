package com.amazon.ion.benchmark;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Represents a combination of write command options that corresponds to a single write benchmark trial.
 */
class WriteOptionsCombination extends OptionsCombinationBase {

    /**
     * @param serializedOptionsCombination text Ion representation of the options combination.
     */
    WriteOptionsCombination(String serializedOptionsCombination) {
        super(serializedOptionsCombination);
    }

    @Override
    protected MeasurableTask createMeasurableTask(Path inputFile) throws IOException {
        return format.createWriteTask(inputFile, this);
    }
}
