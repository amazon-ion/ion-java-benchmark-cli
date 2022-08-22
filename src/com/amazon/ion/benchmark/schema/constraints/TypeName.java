package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;

public class TypeName implements ReparsedConstraint {
    private IonType typeName;

    /**
     * Initializing the newly created TypeName object.
     * @param field represents the value of constraint 'type'.
     */
    private TypeName(IonValue field) {
        this.typeName = IonType.valueOf(field.toString().toUpperCase());
    }

    /**
     * Helping access the private attribute 'type'.
     * @return the attribute 'type'.
     */
    public IonType getTypeName() {
        return this.typeName;
    }

    /**
     * Parsing the value of constraint 'type' into TypeName.
     * @param field represents the value of constraint 'type'.
     * @return the parsed object TypeName.
     */
    public static TypeName of(IonValue field) {
        return new TypeName(field);
    }
}
