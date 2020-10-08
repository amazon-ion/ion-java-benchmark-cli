package com.amazon.ion.benchmark;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.amazon.ion.benchmark.Constants.ION_READER_NAME;
import static com.amazon.ion.benchmark.Constants.ION_USE_LOB_CHUNKS_NAME;
import static com.amazon.ion.benchmark.Constants.PATHS_NAME;

/**
 * Represents a combination of read command options that corresponds to a single read benchmark trial.
 */
class ReadOptionsCombination extends OptionsCombinationBase {

    final List<String> paths;
    final IonReaderType readerType;
    final boolean useLobChunks;

    /**
     * @param serializedOptionsCombination text Ion representation of the options combination.
     * @throws IOException if thrown while parsing the options combination.
     */
    ReadOptionsCombination(String serializedOptionsCombination) throws IOException {
        super(serializedOptionsCombination);
        IonStruct optionsCombinationStruct = (IonStruct) Constants.ION_SYSTEM.singleValue(serializedOptionsCombination);
        String pathsFile = getOrDefault(optionsCombinationStruct, PATHS_NAME, val -> ((IonText) val).stringValue(), null);
        if (pathsFile == null) {
            paths = null;
        } else {
            paths = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(pathsFile)))) {
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    paths.add(line);
                    line = reader.readLine();
                }
            }
        }
        readerType = getOrDefault(
            optionsCombinationStruct,
            ION_READER_NAME,
            val -> IonReaderType.valueOf(((IonText) val).stringValue()),
            IonReaderType.NON_BLOCKING
        );
        useLobChunks = getOrDefault(optionsCombinationStruct, ION_USE_LOB_CHUNKS_NAME, val -> ((IonBool) val).booleanValue(), false);
    }

    @Override
    protected MeasurableTask createMeasurableTask(Path inputFile) throws IOException {
        return format.createReadTask(inputFile, this);
    }

}
