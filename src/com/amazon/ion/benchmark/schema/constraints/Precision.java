package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class Precision extends QuantifiableConstraints{

    /**
     * Initializing the Precision object.
     * @param value represents constraint field 'precision'.
     */
    public Precision(IonValue value) {
        super(value);
    }

    /**
     * Parsing constraint field into Precision.
     * @param field represents the value of constraint 'precision'.
     * @return the newly created Precision object.
     */
    public static Precision of(IonValue field) {
        return new Precision(field);
    }
}
