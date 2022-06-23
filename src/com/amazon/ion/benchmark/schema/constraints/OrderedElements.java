package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.schema.ReparsedType;
import com.amazon.ion.system.IonSystemBuilder;

import java.util.ArrayList;

/**
 * Process the constraint 'ordered_elements' and provide the relevant functionalities.
 */
public class OrderedElements implements ReparsedConstraint {
    private final static String KEYWORD_TYPE = "type";
    private final ArrayList<ReparsedType> orderedElementsConstraints;

    /**
     * Initializing the newly created OrderedElements object.
     * @param field represents the value of constraint 'ordered_elements'.
     */
    public OrderedElements(IonValue field) {
        orderedElementsConstraints = new ArrayList<>();
        IonList orderedElements = (IonList) field;
        orderedElements.forEach(this::handleFiled);
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
    private void handleFiled(IonValue type) {
        // The element in 'ordered_elements' constraint is in IonSymbol format when it represents <TYPE_NAME> OR <TYPE_ALIAS>.
        // When the element in 'ordered_elements' constraint represents <<UNNAMED_TYPE_DEFINITION>> or <IMPORT_TYPE>, the value is in IonStruct format.
        if (type instanceof IonSymbol) {
            IonStruct constructedType = IonSystemBuilder.standard().build().newEmptyStruct();
            IonValue typeClone = type.clone();
            constructedType.add(KEYWORD_TYPE, typeClone);
            this.orderedElementsConstraints.add(new ReparsedType(constructedType));
        } else {
            this.orderedElementsConstraints.add(new ReparsedType((IonStruct)type));
        }
    }
}
