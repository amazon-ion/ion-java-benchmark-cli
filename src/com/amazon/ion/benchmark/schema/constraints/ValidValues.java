package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonList;
import com.amazon.ion.IonValue;

// This class is used for parsing constraint 'valid_values' and providing the utilities of processing the value of constraint.
// valid_values: [ <VALUE>... ]
// valid_values: <RANGE<NUMBER>>
// valid_values: <RANGE<TIMESTAMP>>
public class ValidValues implements ReparsedConstraint {
    // TODO: Handling min and max value
    final private IonList validValues;
    final private Range range;
    final private boolean isRange;

    /**
     * Initializing the newly created ValidValues object.
     * @param validValues represents the value of constraint 'valid_values'.
     * @param isRange is a boolean value to represent the format of 'valid_values'.
     */
    public ValidValues(IonList validValues, boolean isRange) {
        this.validValues = isRange ? null : validValues;
        this.range = isRange ? Range.of(validValues) : null;
        this.isRange = isRange;
    }

    /**
     * Getting the value of constraint 'valid_values' in IonList format.
     * @return an IonList which represents the value of constraint 'valid_values'.
     */
    public IonList getValidValues() {
        return validValues;
    }

    /**
     * Checking whether constraint 'valid_values' contains range.
     * @return a boolean value to represent whether 'valid_values' contains range.
     */
    public boolean isRange() {
        return isRange;
    }

    /**
     * Getting the range value of constraint 'valid_values' if its format is one of [<RANGE<NUMBER>> | <RANGE<TIMESTAMP>>]
     * @return a Range object.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Parsing constraint field into ValidValues format.
     * @param field represents constraint field 'valid_values'.
     * @return the newly created ValidValues object.
     */
    public static ValidValues of(IonValue field) {
        boolean isRange = Range.isRange(field);
        return new ValidValues((IonList) field, isRange);
    }
}
