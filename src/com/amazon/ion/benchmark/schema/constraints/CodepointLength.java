package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class CodepointLength extends QuantifiableConstraints {

    /**
     * Initializing the CodepointLength object.
     * @param value represents constraint field 'codepoint_length'.
     */
    public CodepointLength(IonValue value) {
        super(value);
    }

    /**
     * Parsing constraint field into CodepointLength.
     * @param field represents the value of constraint 'codepoint_length'.
     * @return the newly created CodepointLength object.
     */
    public static CodepointLength of(IonValue field) {
       return new CodepointLength(field);
    }
}
