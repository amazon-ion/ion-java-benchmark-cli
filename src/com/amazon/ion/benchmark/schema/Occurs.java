package com.amazon.ion.benchmark.schema;

import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.schema.constraints.QuantifiableConstraints;
import com.amazon.ion.benchmark.schema.constraints.Range;
import com.amazon.ion.system.IonSystemBuilder;

/**
 * The 'occurs' field indicates either the exact or minimum/maximum number of occurrences of the specified type or field.
 * 'occurs' will not be considered as a constraint. It works with constraints 'fields' or 'ordered_elements' together to specify the generated data.
 * The special value optional is synonymous with range::[0, 1]; similarly, the special value required is synonymous with the exact value 1 (or range::[1, 1]).
 */
public class Occurs extends QuantifiableConstraints {
    private final IonValue occurValue;
    private final static IonSystem SYSTEM = IonSystemBuilder.standard().build();

    /**
     * Initializing the newly created Occurs object.
     * @param field represents the value of 'occurs'.
     */
    private Occurs(IonValue field) {
        super(field);
        this.occurValue = field;
    }

    /**
     * Helping to access the value that field 'occurs' represents.
     * @return a Range which represents the range of occurrences.
     */
    public Range getOccurRange() {
        if (this.occurValue instanceof IonSymbol) {
            String occurValue = this.occurValue.toString();
            switch (occurValue) {
                // occurs: optional equivalent to range::[0, 1]
                case "optional":
                    return new Range(SYSTEM.newList(SYSTEM.newInt(0), SYSTEM.newInt(1)));
                // occurs: required equivalent to 1 or range::[1, 1]
                case "required":
                    return new Range(SYSTEM.newList(SYSTEM.newInt(1), SYSTEM.newInt(1)));
                default:
                    throw new IllegalStateException("The symbol value cannot be processed.");
            }
        } else {
            return getRange();
        }
    }

    /**
     * Parsing the value of 'occurs' into Occurs.
     * @param value represents the value of 'occurs'.
     * @return the newly created object Occurs.
     */
    public static Occurs of(IonValue value) {
        return new Occurs(value);
    }

}
