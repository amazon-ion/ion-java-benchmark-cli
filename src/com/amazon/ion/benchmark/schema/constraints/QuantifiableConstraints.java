package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

// This class is used for processing the constraints [codepoint_length | byte_length | precision | scale | container_length].
// These constraints have two formats of value [<INT> | <RANGE<INT>>].
public class QuantifiableConstraints implements ReparsedConstraint {
    private final Range range;

    /**
     * Initializing the newly created QuantifiableConstraint object.
     * @param value represents one of [codepoint_length | byte_length | precision | scale | container_length] field value.
     */
    public QuantifiableConstraints(IonValue value) {
        this.range = Range.of(value);
    }

    /**
     * Parsing constraint field into QuantifiableConstraints.
     * @param field represents the value of constraint.
     * @return newly created QuantifiableConstraints object.
     */
    public static QuantifiableConstraints of(IonValue field) {
        return new QuantifiableConstraints(field);
    }

    /**
     * Getting the range value if the constraint value contains annotation 'range'.
     * @return object Range which represents constraint value.
     */
    public Range getRange() {
        return range;
    }
}
