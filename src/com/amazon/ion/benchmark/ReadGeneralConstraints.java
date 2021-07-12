package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Map;

/**
 * Parse Ion Schema file and get the general constraints in the file then pass the constraints to the Ion data generator.
 */
public class ReadGeneralConstraints {
    public static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    public static final IonLoader LOADER = SYSTEM.newLoader();
    private static final String DEFAULT_RANGE = "[0, 1114111]";

    /**
     * Get general constraints of Ion Schema and call the relevant generator method based on the type.
     * @param size is the size of the output file.
     * @param path is the path of the Ion Schema file.
     * @param format is the format of the generated file, select from set (ion_text | ion_binary).
     * @param outputFile is the path of the generated file.
     * @throws Exception if errors occur when reading and writing data.
     */
    public static void readIonSchemaAndGenerate(int size, String path, String format, String outputFile) throws Exception {
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(path)))) {
            IonDatagram schema = LOADER.load(reader);
            for (int i = 0; i < schema.size(); i++) {
                IonValue schemaValue = schema.get(i);
                // Assume there's only one constraint between schema_header and schema_footer, if more constraints added, here is the point where developers should start.
                if (schemaValue.getType().equals(IonType.STRUCT) && schemaValue.getTypeAnnotations()[0].equals(IonSchemaUtilities.KEYWORD_TYPE)) {
                    IonStruct constraintStruct = (IonStruct) schemaValue;
                    // Get general constraints:
                    IonType type = IonType.valueOf(constraintStruct.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
                    Map<String, Object> annotationMap = IonSchemaUtilities.getAnnotation(constraintStruct);
                    // If more types of Ion data added in the future, developers can add more types under the switch logic.
                    switch (type) {
                        case STRUCT:
                            // If more constraints relevant to Ion Struct needed to be processed, developers should call functions here.
                            IonStruct fields = (IonStruct) constraintStruct.get(IonSchemaUtilities.KEYWORD_FIELDS);
                            WriteRandomIonValues.writeRandomStructValues(size, format, outputFile, fields, annotationMap);
                            break;
                        case TIMESTAMP:
                            WriteRandomIonValues.writeRandomTimestamps(size, type, outputFile, null, format);
                            break;
                        case STRING:
                            int codePointBoundary = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_CODE_POINT_LENGTH);
                            WriteRandomIonValues.writeRandomStrings(size, type, outputFile, DEFAULT_RANGE, format, codePointBoundary);
                            break;
                        case DECIMAL:
                            break;
                        case INT:
                            WriteRandomIonValues.writeRandomInts(size, type, format, outputFile);
                            break;
                        case FLOAT:
                            WriteRandomIonValues.writeRandomFloats(size, type, format, outputFile);
                            break;
                        case BLOB:
                        case CLOB:
                            WriteRandomIonValues.writeRandomLobs(size, type, format, outputFile);
                            break;
                        case SYMBOL:
                            WriteRandomIonValues.writeRandomSymbolValues(size, format, outputFile);
                            break;
                        case LIST:
                            WriteRandomIonValues.writeRandomListValues(size, format, outputFile, constraintStruct, annotationMap);
                            break;
                        default:
                            throw new IllegalStateException(type + " is not supported when generating IonValue based on Ion Schema.");
                    }
                }
            }
        }
    }
}
