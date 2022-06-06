package com.amazon.ion.benchmark;

import com.amazon.ion.IonLoader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.benchmark.schema.ReparsedType;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ionschema.Schema;
import com.amazon.ionschema.Type;
import com.google.common.io.CountingOutputStream;

import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Parse Ion Schema file and extract the type definition as ReparsedType object then pass the re-parsed type definition to the Ion data generator.
 */
public class ReadGeneralConstraints {
    public static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    public static final IonLoader LOADER = SYSTEM.newLoader();

    /**
     * Getting the constructed data which is conformed with ISL and writing data to the output file.
     * @param size is the size of the output file.
     * @param schema an Ion Schema loaded by ion-schema-kotlin.
     * @param format is the format of the generated file, select from set (ion_text | ion_binary).
     * @param outputFile is the path of the generated file.
     * @throws Exception if errors occur when writing data.
     */
    public static void constructAndWriteIonData(int size, Schema schema, String format, String outputFile) throws Exception {
        // Assume there's only one type definition between schema_header and schema_footer.
        // If more constraints added, here is the point where developers should start.
        Type schemaType = schema.getTypes().next();
        ReparsedType parsedTypeDefinition = new ReparsedType(schemaType);
        CountingOutputStream outputStreamCounter = new CountingOutputStream(new FileOutputStream(outputFile));
        long count = 0;
        try (IonWriter writer = formatWriter(format, outputStreamCounter)) {
            while (count <= size) {
                IonValue constructedData = DataConstructor.constructIonData(parsedTypeDefinition);
                constructedData.writeTo(writer);
                count = outputStreamCounter.getCount();
            }
        }
        // Print the successfully generated data notification which includes the file path information.
        DataConstructor.printInfo(outputFile);
    }

    /**
     * Construct the writer based on the provided format (ion_text|ion_binary).
     * @param format decides which writer should be constructed.
     * @param outputStream represents the bytes stream which will be written into the output file.
     * @return the writer which conforms with the required format.
     */
    public static IonWriter formatWriter(String format, OutputStream outputStream) {
        IonWriter writer;
        Format formatName = Format.valueOf(format.toUpperCase());
        switch (formatName) {
            case ION_BINARY:
                writer = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(outputStream);
                break;
            case ION_TEXT:
                writer = IonTextWriterBuilder.standard().build(outputStream);
                break;
            default:
                throw new IllegalStateException("Please input the format ion_text or ion_binary");
        }
        return writer;
    }
}
