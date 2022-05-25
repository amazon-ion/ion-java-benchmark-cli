package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class Scale extends QuantifiableConstraints{

    /**
     * Initializing the Scale object.
     * @param value represents constraint field 'scale'.
     */
    public Scale(IonValue value) {
        super(value);
    }

    /**
     * Parsing constraint field into Scale.
     * @param field represents the value of constraint 'scale'.
     * @return the newly created Scale object.
     */
    public static Scale of(IonValue field) {
        return new Scale(field);
    }
}
