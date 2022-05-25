package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class ByteLength extends QuantifiableConstraints{

    /**
     * Initializing the ByteLength object.
     * @param value represents constraint field 'byte_length'.
     */
    public ByteLength(IonValue value) {
        super(value);
    }

    /**
     * Parsing constraint field into ByteLength.
     * @param field represents the value of constraint 'byte_length'.
     * @return the newly created ByteLength object.
     */
    public static ByteLength of(IonValue field) {
        return new ByteLength(field);
    }
}
