package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class ContainerLength extends QuantifiableConstraints{

    /**
     * Initializing the ContainerLength object.
     * @param value represents constraint field 'container_length'.
     */
    private ContainerLength(IonValue value) {
        super(value);
    }

    /**
     * Parsing constraint field into ContainerLength.
     * @param field represents the value of constraint 'container_length'.
     * @return newly created ContainerLength object.
     */
    public static ContainerLength of(IonValue field) {
        return new ContainerLength(field);
    }
}
