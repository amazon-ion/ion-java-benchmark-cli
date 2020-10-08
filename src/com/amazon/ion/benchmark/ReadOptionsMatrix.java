package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;

import java.util.List;
import java.util.Map;

import static com.amazon.ion.benchmark.Constants.ION_READER_NAME;
import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;
import static com.amazon.ion.benchmark.Constants.ION_USE_LOB_CHUNKS_NAME;
import static com.amazon.ion.benchmark.Constants.PATHS_NAME;

/**
 * Represents all read command options combinations, corresponding to all read benchmark trials. A single
 * ReadOptionsMatrix may yield multiple ReadOptionsCombinations.
 */
class ReadOptionsMatrix extends OptionsMatrixBase {

    /**
     * @param optionsMatrix Map representing the options matrix for this command. The values of the map are either
     *                      scalar values or Lists of scalar values.
     */
    ReadOptionsMatrix(Map<String, Object> optionsMatrix) {
        super("read", optionsMatrix);
    }

    @Override
    void parseCommandSpecificOptions(Map<String, Object> optionsMatrix, List<IonStruct> optionsCombinationStructs) {
        String pathsFile = getStringOrNull(optionsMatrix.get("--paths"));
        if (pathsFile != null) {
            addOptionTo(optionsCombinationStructs, PATHS_NAME, ION_SYSTEM.newString(requireFileToExist(pathsFile)));
        }
        parseAndCombine(
            optionsMatrix.get("--ion-reader"),
            ION_READER_NAME,
            (s) -> IonReaderType.valueOf(s.toUpperCase()),
            (type) -> ION_SYSTEM.newSymbol(type.name()),
            optionsCombinationStructs,
            OptionsMatrixBase::noImplicitDefault
        );
        parseAndCombine(
            optionsMatrix.get("--ion-use-lob-chunks"),
            ION_USE_LOB_CHUNKS_NAME,
            OptionsMatrixBase::getTrueOrNull,
            ION_SYSTEM::newBool,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newBool(false)
        );
    }

}
