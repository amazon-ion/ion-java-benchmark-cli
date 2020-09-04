package com.amazon.ion.benchmark;

import com.amazon.ion.IonStruct;

import java.util.List;
import java.util.Map;

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
        // There are currently no write-specific options.
    }

}
