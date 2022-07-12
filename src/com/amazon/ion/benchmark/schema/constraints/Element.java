package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.IonSchemaUtilities;
import com.amazon.ion.benchmark.schema.ReparsedType;

public class Element implements ReparsedConstraint {
    private ReparsedType element;

    /**
     * Initializing the newly created Element object.
     * @param field represents the value of constraint 'element'.
     */
    private Element (IonValue field) {
        this.element = IonSchemaUtilities.parseTypeReference(field);
    }

    /**
     * Parsing the value of constraint 'element' into Element.
     * @param field represents the value of constraint 'element'.
     * @return the parsed object Element.
     */
    public static Element of (IonValue field) {
        return new Element(field);
    }

    /**
     * Helping access the private attribute 'element'.
     * @return the attribute 'element'.
     */
    public ReparsedType getElement() {
        return this.element;
    }
}
