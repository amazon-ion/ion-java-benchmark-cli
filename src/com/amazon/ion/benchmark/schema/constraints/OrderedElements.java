package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonList;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.IonSchemaUtilities;
import com.amazon.ion.benchmark.schema.ReparsedType;

import java.util.ArrayList;

/**
 * Process the constraint 'ordered_elements' and provide the relevant functionalities.
 */
public class OrderedElements implements ReparsedConstraint {
    private final ArrayList<ReparsedType> orderedElementsConstraints;

    /**
     * Initializing the newly created OrderedElements object.
     * @param field represents the value of constraint 'ordered_elements'.
     */
    private OrderedElements(IonValue field) {
        orderedElementsConstraints = new ArrayList<>();
        IonList orderedElements = (IonList) field;
        orderedElements.forEach(this::handleField);
    }

    /**
     * Parsing the value of constraint 'ordered_elements' into OrderedElements.
     * @param field represents the value of constraint 'ordered_elements'.
     * @return the newly created OrderedElements object.
     */
    public static OrderedElements of(IonValue field) {
        return new OrderedElements(field);
    }

    /**
     * Helping access the private attribute orderedElementsConstraints.
     * @return an ArrayList which contains the constraints of elements.
     */
    public ArrayList<ReparsedType> getOrderedElementsConstraints() {
        return this.orderedElementsConstraints;
    }

    /**
     * Parsing each field into ReparsedType then collecting them into an ArrayList.
     * @param type represents the element contained in the constraint value container of 'ordered_elements'.
     */
    private void handleField(IonValue type) {
        this.orderedElementsConstraints.add(IonSchemaUtilities.parseTypeReference(type));
    }
}
