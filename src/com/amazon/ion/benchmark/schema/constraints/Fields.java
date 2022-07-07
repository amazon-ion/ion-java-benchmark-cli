package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.IonSchemaUtilities;
import com.amazon.ion.benchmark.schema.ReparsedType;

import java.util.HashMap;
import java.util.Map;

/** Constraint description:
 *  fields: { <FIELD>... }
 *  <FIELD> is defined as: <SYMBOL>: <VARIABLY_OCCURRING_TYPE>
 *  <VARIABLY_OCCURRING_TYPE_REFERENCE> ::= type::{ <OCCURS>, <CONSTRAINT>... }
 *                                          | { <OCCURS>, <CONSTRAINT>... }
 *                                          | <TYPE_REFERENCE>
 */
public class Fields implements ReparsedConstraint {
    // Creating a hashMap containing field information.
    // The key of fieldMap represents the field name.
    // The value of fieldMap represent the constraint value of each field.
    private final Map<String, ReparsedType> fieldMap;

    /**
     * Initializing the newly created Fields object.
     * @param field represents the value of constraint 'fields'.
     */
    private Fields(IonValue field) {
        fieldMap = new HashMap<>();
        IonStruct fieldsStruct = (IonStruct) field;
        fieldsStruct.forEach(this::handleField);
    }

    /**
     * Parsing the value of each field in constraint 'fields' to ReparsedType and collecting all parsed value into a hashMap.
     * @param value represents the value of the field contained in constraint 'fields'.
     */
    private void handleField(IonValue value) {
        fieldMap.put(value.getFieldName(), IonSchemaUtilities.parseTypeReference(value));
    }

    /**
     * Helping to access the private attribute fieldMap.
     * @return fieldMap which contains information of constraint 'fields'.
     */
    public Map<String, ReparsedType> getFieldMap() {
        return this.fieldMap;
    }

    /**
     * Parsing the value of constraint 'fields' into Fields.
     * @param value represents the value of constraint 'fields'.
     * @return the object of Fields.
     */
    public static Fields of(IonValue value) {
        return new Fields(value);
    }
}
