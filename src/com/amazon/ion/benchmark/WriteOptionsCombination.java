package com.amazon.ion.benchmark;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;

import java.io.IOException;
import java.nio.file.Path;

import static com.amazon.ion.benchmark.Constants.ION_WRITER_BLOCK_SIZE_NAME;

/**
 * Represents a combination of write command options that corresponds to a single write benchmark trial.
 */
class WriteOptionsCombination extends OptionsCombinationBase {

    final Integer ionWriterBlockSize;

    /**
     * @param serializedOptionsCombination text Ion representation of the options combination.
     */
    WriteOptionsCombination(String serializedOptionsCombination) {
        super(serializedOptionsCombination);
        IonStruct optionsCombinationStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(serializedOptionsCombination);
        ionWriterBlockSize = getOrDefault(optionsCombinationStruct, ION_WRITER_BLOCK_SIZE_NAME, val -> ((IonInt) val).intValue(), null);
    }

    @Override
    protected MeasurableTask createMeasurableTask(Path inputFile) throws IOException {
        return format.createWriteTask(inputFile, this);
    }
}
