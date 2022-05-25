package com.amazon.ion.benchmark;

import com.amazon.ion.IonLoader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.benchmark.schema.ReparsedType;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionschema.Schema;
import com.amazon.ionschema.Type;

import java.io.File;

/**
 * Parse Ion Schema file and extract the type definition as ReparsedType object then pass the re-parsed type definition to the Ion data generator.
 */
public class ReadGeneralConstraints {
    public static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    public static final IonLoader LOADER = SYSTEM.newLoader();

    /**
     * Parsing schema type definition to ReparsedType and passing the re-parsed value to data generating process.
     * @param size is the size of the output file.
     * @param schema an Ion Schema loaded by ion-schema-kotlin.
     * @param format is the format of the generated file, select from set (ion_text | ion_binary).
     * @param outputFile is the path of the generated file.
     * @throws Exception if errors occur when writing data.
     */
    public static void readIonSchemaAndGenerate(int size, Schema schema, String format, String outputFile) throws Exception {
        // Assume there's only one constraint between schema_header and schema_footer.
        // If more constraints added, here is the point where developers should start.
        Type schemaType = schema.getTypes().next();
        ReparsedType parsedTypeDefinition = new ReparsedType(schemaType);
        File file = new File(outputFile);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, parsedTypeDefinition);
        }
        // Print the successfully generated data notification which includes the file path information.
        WriteRandomIonValues.printInfo(outputFile);
    }
}
