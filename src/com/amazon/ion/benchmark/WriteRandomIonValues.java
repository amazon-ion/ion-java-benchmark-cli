package com.amazon.ion.benchmark;

/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazon.ion.Decimal;
//import com.amazon.ion.IonBufferConfiguration;
//import com.amazon.ion.IonBufferEventHandler;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class WriteRandomIonValues {

    private static void writeRandomStrings() throws Exception {
        File file = new File("randomStrings.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Strings will average around 20 bytes (2 bytes on average for each code point,
            // average of 10 code points per string).
            for (int i = 0; i < ( 1000000/ 20); i++) {
                int length = random.nextInt(20);
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < length; j++) {
                    int codePoint;
                    int type;
                    do {
                        codePoint = random.nextInt(Character.MAX_CODE_POINT);
                        type = Character.getType(codePoint);
                    } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
                    sb.appendCodePoint(codePoint);
                }
                writer.writeString(sb.toString());
            }
        }
        System.out.println("Finished writing strings. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            while (reader.next() != null) {
                if (reader.getType() != IonType.STRING) {
                    throw new IllegalStateException("Found non-string");
                }
                reader.stringValue();
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    public static void writeRandomDecimals(int size, String file_name) throws Exception {
        File file = new File(file_name);
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Decimals will average around 8 bytes, and we're writing 3 per iteration.
            for (int i = 0; i < (size / 8 / 3); i++) {
                int randomScale = random.nextInt(2048);
                randomScale -= 1024;
                writer.writeDecimal(BigDecimal.valueOf(random.nextDouble()).scaleByPowerOfTen(randomScale));
                writer.writeDecimal(BigDecimal.valueOf(random.nextInt()));
                writer.writeDecimal(BigDecimal.valueOf(random.nextFloat()).scaleByPowerOfTen(randomScale));
            }
        }
        System.out.println("Finished writing decimals. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.DECIMAL) {
                    throw new IllegalStateException("Found non-decimal");
                }
                Decimal value = reader.decimalValue();
                if (i++ < 100) {
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static void writeRandomInts() throws Exception {
        File file = new File("randomInts.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Ints will average around 8 bytes, and we're writing 4 per iteration.
            for (int i = 0; i < (100_000_000 / 8 / 3); i++) {
                writer.writeInt(random.nextInt(1024));
                writer.writeInt(random.nextInt());
                long longValue = random.nextLong();
                writer.writeInt(longValue);
                writer.writeInt(BigInteger.valueOf(longValue).multiply(BigInteger.TEN));
            }
        }
        System.out.println("Finished writing ints. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.INT) {
                    throw new IllegalStateException("Found non-int");
                }
                Decimal value = reader.decimalValue();
                if (i++ < 100) {
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static void writeRandomFloats() throws Exception {
        File file = new File("randomFloats.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().withFloatBinary32Enabled().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Floats will average at 7 bytes, and we're writing 2 per iteration.
            for (int i = 0; i < (100_000_000 / 7 / 2); i++) {
                writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
                writer.writeFloat(Float.intBitsToFloat(random.nextInt()));
            }
        }
        System.out.println("Finished writing floats. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.FLOAT) {
                    throw new IllegalStateException("Found non-float");
                }
                double value = reader.doubleValue();
                if (i++ < 100) {
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static void writeListOfRandomFloats() throws Exception {
        File file = new File("manyLargeListsOfRandomFloats.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().withFloatBinary32Enabled().build(out)
        ) {
            Random random = new Random();
            for (int j = 0; j < 100; j++) {
                writer.stepIn(IonType.LIST);
                // Target about 1MB of data. Floats will average at 7 bytes, and we're writing 2 per iteration.
                for (int i = 0; i < (1_000_000 / 7 / 2); i++) {
                    writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
                    writer.writeFloat(Float.intBitsToFloat(random.nextInt()));
                }
                writer.stepOut();
            }
        }
        System.out.println("Finished writing floats. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            while (reader.next() != null) {
                reader.stepIn();
                while (reader.next() != null) {
                    if (reader.getType() != IonType.FLOAT) {
                        throw new IllegalStateException("Found non-float");
                    }
                    double value = reader.doubleValue();
                }
                reader.stepOut();
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static Integer randomLocalOffset(Random random) {
        // Offsets are in minutes, [-23:59, 23:59], i.e. [-1439, 1439].
        // The most common offset is Z (00:00), while unknown (-00:00) may also be common.
        Integer offsetMinutes = random.nextInt(6000) - 2000;
        if (offsetMinutes > 1439) {
            // This means about 43% of timestamps will have offset Z (UTC).
            offsetMinutes = 0;
        } else if (offsetMinutes < -1439) {
            // This means about 9% of timestamps will have unknown offset (-00:00).
            offsetMinutes = null;
        }
        return offsetMinutes;
    }

    private static BigDecimal randomSecondWithFraction(Random random) {
        int second = random.nextInt(60);
        int fractional = Math.abs(random.nextInt(Integer.MAX_VALUE));
        int numberOfDigits = (int) Math.ceil(Math.log10(fractional));
        BigDecimal fractionalSecond = BigDecimal.valueOf(fractional, numberOfDigits);
        return fractionalSecond.add(BigDecimal.valueOf(second));
    }

    private static void writeRandomTimestamps() throws Exception {
        File file = new File("randomTimestamps.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Timestamps will average around 7 bytes.
            Timestamp.Precision[] precisions = Timestamp.Precision.values();
            for (int i = 0; i < (100_000_000 / 7); i++) {
                Timestamp.Precision precision = precisions[random.nextInt(precisions.length)];
                Timestamp timestamp;
                switch (precision) {
                    case YEAR:
                        timestamp = Timestamp.forYear(random.nextInt(9998) + 1);
                        break;
                    case MONTH:
                        timestamp = Timestamp.forMonth(random.nextInt(9998) + 1, random.nextInt(12) + 1);
                        break;
                    case DAY:
                        timestamp = Timestamp.forDay(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1 // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                        );
                        break;
                    case MINUTE:
                        timestamp = Timestamp.forMinute(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                            random.nextInt(24),
                            random.nextInt(60),
                            randomLocalOffset(random)
                        );
                        break;
                    case SECOND:
                        timestamp = Timestamp.forSecond(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                            random.nextInt(24),
                            random.nextInt(60),
                            random.nextInt(60),
                            randomLocalOffset(random)
                        );
                        break;
                    case FRACTION:
                        timestamp = Timestamp.forSecond(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                            random.nextInt(24),
                            random.nextInt(60),
                            randomSecondWithFraction(random),
                            randomLocalOffset(random)
                        );
                        break;
                    default:
                        throw new IllegalStateException();
                }
                writer.writeTimestamp(timestamp);
            }
        }
        System.out.println("Finished writing timestamps. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.TIMESTAMP) {
                    throw new IllegalStateException("Found non-timestamp");
                }
                Timestamp value = reader.timestampValue();
                if (i++ < 100) {
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static void writeRandomLobs(IonType type) throws IOException {
        File file = new File("randomClobs.10n");
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            Random random = new Random();
            // Target about 100MB of data. Blobs will average around 259 bytes.
            for (int i = 0; i < 100_000_000 / 259; i++) {
                byte[] randomBytes = new byte[random.nextInt(512)];
                random.nextBytes(randomBytes);
                if (type == IonType.BLOB) {
                    writer.writeBlob(randomBytes);
                } else {
                    writer.writeClob(randomBytes);
                }
            }
        }
        System.out.println("Finished writing lobs. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            while (reader.next() != null) {
                if (reader.getType() != type) {
                    throw new IllegalStateException("Found non-lob");
                }
                byte[] value = reader.newBytes();
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    private static void writeRandomAnnotatedFloats() throws Exception {
        File file = new File("randomAnnotatedFloats.10n");
        List<String> annotations = new ArrayList<>(500);
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            int length = random.nextInt(20);
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < length; j++) {
                int codePoint;
                int type;
                do {
                    codePoint = random.nextInt(Character.MAX_CODE_POINT);
                    type = Character.getType(codePoint);
                } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
                sb.appendCodePoint(codePoint);
            }
            annotations.add(sb.toString());
        }
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            // Target about 100MB of data. Annotated floats will average around 14 bytes.
            for (int i = 0; i < (100_000_000 / 14); i++) {
                // 60% of values will have 1 annotation; 40% will have 2 or 3.
                int numberOfAnnotations = random.nextInt(5) + 1;
                if (numberOfAnnotations > 3) {
                    numberOfAnnotations = 1;
                }
                for (int j = 0; j < numberOfAnnotations; j++) {
                    writer.addTypeAnnotation(annotations.get(random.nextInt(500)));
                }
                writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
            }
        }
        System.out.println("Finished writing floats. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.FLOAT) {
                    throw new IllegalStateException("Found non-float");
                }
                double value = reader.doubleValue();
                if (i++ < 100) {
                    System.out.print(Arrays.toString(reader.getTypeAnnotations()) + " ");
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }
//mark as an example
     public static void writeRandomSymbolValues(int size,String file_name) throws IOException {
        File file = new File(file_name);
        List<String> symbols = new ArrayList<>(500);
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            int length = random.nextInt(20);
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < length; j++) {
                int codePoint;
                int type;
                do {
                    codePoint = random.nextInt(Character.MAX_CODE_POINT);
                    type = Character.getType(codePoint);
                } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
                sb.appendCodePoint(codePoint);
            }
            symbols.add(sb.toString());
        }
        try (
            OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            IonWriter writer = IonBinaryWriterBuilder.standard().build(out)
        ) {
            // Target 100MB of data. Symbol values will average 2 bytes each.
            for (int i = 0; i < size/ 2; i++) {
                writer.writeSymbol(symbols.get(random.nextInt(500)));
            }
        }
        System.out.println("Finished writing symbol values. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            int i = 0;
            while (reader.next() != null) {
                if (reader.getType() != IonType.SYMBOL) {
                    throw new IllegalStateException("Found non-symbol");
                }
                String value = reader.stringValue();
                if (i++ < 100) {
                    System.out.println(value);
                }
            }
        }
        System.out.println("Done. Size: " + file.length());
    }
}