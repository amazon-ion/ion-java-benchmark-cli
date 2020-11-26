package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;

import java.util.List;
import java.util.Map;

import static com.amazon.ion.benchmark.Constants.ION_SYSTEM;
import static com.amazon.ion.benchmark.Constants.ION_WRITER_BLOCK_SIZE_NAME;

/**
 * Represents all write command options combinations, corresponding to all write benchmark trials. A single
 * WriteOptionsMatrix may yield multiple WriteOptionsCombinations.
 */
class WriteOptionsMatrix extends OptionsMatrixBase {

    /**
     * @param optionsMatrix Map representing the options matrix for this command. The values of the map are either
     *                      scalar values or Lists of scalar values.
     */
    WriteOptionsMatrix(Map<String, Object> optionsMatrix) {
        super("write", optionsMatrix);
    }

    @Override
    void parseCommandSpecificOptions(Map<String, Object> optionsMatrix, List<IonStruct> optionsCombinationStructs) {
        parseAndCombine(
            optionsMatrix.get("--ion-writer-block-size"),
            ION_WRITER_BLOCK_SIZE_NAME,
            OptionsMatrixBase::getIntOrAuto,
            ION_SYSTEM::newInt,
            optionsCombinationStructs,
            () -> ION_SYSTEM.newSymbol(Constants.AUTO_VALUE),
            OPTION_ONLY_APPLIES_TO_ION_BINARY
        );
    }

}
