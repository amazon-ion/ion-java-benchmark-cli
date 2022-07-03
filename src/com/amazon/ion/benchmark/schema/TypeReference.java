package com.amazon.ion.benchmark.schema;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;

/**
 * Processing the constraint value <TYPE_REFERENCE>. Here are the constraints that using <TYPE_REFERENCE> as their value:
 * {type: <TYPE_REFERENCE>, element: <TYPE_REFERENCE>, all_of: [ <TYPE_REFERENCE>... ], one_of: [ <TYPE_REFERENCE>... ], not: <TYPE_REFERENCE>}
 * <TYPE_REFERENCE> ::=           <TYPE_NAME>
 *                    | nullable::<TYPE_NAME>
 *                    |           <TYPE_ALIAS>
 *                    | nullable::<TYPE_ALIAS>
 *                    |           <UNNAMED_TYPE_DEFINITION>
 *                    | nullable::<UNNAMED_TYPE_DEFINITION>
 *                    |           <IMPORT_TYPE>
 *                    | nullable::<IMPORT_TYPE>
 *
 */
// TODO: Support processing <TYPE_ALIAS> and <IMPORT_TYPE>
public class TypeReference {
    private ReparsedType typeDefinition;
    private static final String KEYWORD_TYPE = "type";

    /**
     * Initializing the newly created TypeReference instance.
     * @param field represents constraint value <TYPE_REFERENCE>.
     */
    public TypeReference(IonValue field) {
        IonStruct typeDefinition = IonSystemBuilder.standard().build().newEmptyStruct();
        if (field instanceof IonSymbol) {
            IonValue value = field.clone();
            typeDefinition.add(KEYWORD_TYPE, value);
        } else if (field instanceof IonStruct) {
            typeDefinition = (IonStruct)field;
        }
        this.typeDefinition = new ReparsedType(typeDefinition);
    }

    /**
     * Helping to access the private attribute typeDefinition which is reconstructed from the constraint value <TYPE_REFERENCE>.
     * @return the reconstructed type definition.
     */
    public ReparsedType getTypeDefinition() {
        return this.typeDefinition;
    }
}
