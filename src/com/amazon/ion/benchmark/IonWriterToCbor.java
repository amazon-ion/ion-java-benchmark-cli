package com.amazon.ion.benchmark;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;

/**
 * Allows writing CBOR data via the IonWriter interface. Useful for converting Ion data to CBOR.
 */
class IonWriterToCbor implements IonWriter {

    // The CBOR spec uses tag 0 to indicate that the string that follows is an RFC3339 date/time.
    private static final int CBOR_DATETIME_TAG = 0;

    private final CBORGenerator cbor;
    private final Deque<Boolean> isInStructStack = new ArrayDeque<>(10);

    IonWriterToCbor(CBORGenerator cbor) {
        this.cbor = cbor;
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException("CBOR does not use symbol tables.");
    }

    @Override
    public void flush() throws IOException {
        cbor.flush();
    }

    @Override
    public void finish() throws IOException {
        cbor.flush();
    }

    @Override
    public void close() throws IOException {
        cbor.close();
    }

    @Override
    public void setFieldName(String s) {
        try {
            cbor.writeFieldName(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setFieldNameSymbol(SymbolToken symbolToken) {
        setFieldName(symbolToken.assumeText());
    }

    @Override
    public void setTypeAnnotations(String... strings) {
        // This is lossy. CBOR does not have annotations.
    }

    @Override
    public void setTypeAnnotationSymbols(SymbolToken... symbolTokens) {
        // This is lossy. CBOR does not have annotations.
    }

    @Override
    public void addTypeAnnotation(String s) {
        // This is lossy. CBOR does not have annotations.
    }

    @Override
    public void stepIn(IonType ionType) throws IOException {
        if (ionType == IonType.STRUCT) {
            cbor.writeStartObject();
            isInStructStack.push(Boolean.TRUE);
        } else if (IonType.isContainer(ionType)) {
            cbor.writeStartArray();
            isInStructStack.push(Boolean.FALSE);
        } else {
            throw new IllegalArgumentException("stepIn() requires a container type.");
        }
    }

    @Override
    public void stepOut() throws IOException {
        if (isInStructStack.pop()) {
            cbor.writeEndObject();
        } else {
            cbor.writeEndArray();
        }
    }

    @Override
    public boolean isInStruct() {
        return !isInStructStack.isEmpty() && isInStructStack.peek();
    }

    @Override
    public void writeValue(IonValue ionValue) throws IOException {
        throw new UnsupportedEncodingException("This could be supported, but it currently is not.");
    }

    @Override
    public void writeValue(final IonReader reader) throws IOException {
        writeValueRecursive(reader);
    }

    private void writeValueRecursive(final IonReader reader) throws IOException {
        final IonType type = reader.getType();

        final SymbolToken fieldName = reader.getFieldNameSymbol();
        if (fieldName != null && isInStruct()) {
            setFieldNameSymbol(fieldName);
        }
        final SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        if (annotations.length > 0) {
            setTypeAnnotationSymbols(annotations);
        }
        if (reader.isNullValue()) {
            writeNull(type);
            return;
        }

        switch (type) {
            case BOOL:
                final boolean booleanValue = reader.booleanValue();
                writeBool(booleanValue);
                break;
            case INT:
                switch (reader.getIntegerSize()) {
                    case INT:
                        final int intValue = reader.intValue();
                        writeInt(intValue);
                        break;
                    case LONG:
                        final long longValue = reader.longValue();
                        writeInt(longValue);
                        break;
                    case BIG_INTEGER:
                        final BigInteger bigIntegerValue = reader.bigIntegerValue();
                        writeInt(bigIntegerValue);
                        break;
                    default:
                        throw new IllegalStateException();
                }
                break;
            case FLOAT:
                final double doubleValue = reader.doubleValue();
                writeFloat(doubleValue);
                break;
            case DECIMAL:
                final Decimal decimalValue = reader.decimalValue();
                writeDecimal(decimalValue);
                break;
            case TIMESTAMP:
                final Timestamp timestampValue = reader.timestampValue();
                writeTimestamp(timestampValue);
                break;
            case SYMBOL:
                final SymbolToken symbolToken = reader.symbolValue();
                writeSymbolToken(symbolToken);
                break;
            case STRING:
                final String stringValue = reader.stringValue();
                writeString(stringValue);
                break;
            case CLOB:
                final byte[] clobValue = reader.newBytes();
                writeClob(clobValue);
                break;
            case BLOB:
                final byte[] blobValue = reader.newBytes();
                writeBlob(blobValue);
                break;
            case LIST:
            case SEXP:
            case STRUCT:
                reader.stepIn();
                stepIn(type);
                while (reader.next() != null) {
                    writeValue(reader);
                }
                stepOut();
                reader.stepOut();
                break;
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    @Override
    public void writeValues(final IonReader reader) throws IOException {
        if (reader.getType() != null) {
            writeValue(reader);
        }
        while (reader.next() != null) {
            writeValue(reader);
        }
    }

    @Override
    public void writeNull() throws IOException {
        cbor.writeNull();
    }

    @Override
    public void writeNull(IonType ionType) throws IOException {
        cbor.writeNull();
    }

    @Override
    public void writeBool(boolean b) throws IOException {
        cbor.writeBoolean(b);
    }

    @Override
    public void writeInt(long l) throws IOException {
        cbor.writeNumber(l);
    }

    @Override
    public void writeInt(BigInteger bigInteger) throws IOException {
        cbor.writeNumber(bigInteger);
    }

    @Override
    public void writeFloat(double v) throws IOException {
        cbor.writeNumber(v);
    }

    @Override
    public void writeDecimal(BigDecimal bigDecimal) throws IOException {
        cbor.writeNumber(bigDecimal);
    }

    @Override
    public void writeTimestamp(Timestamp timestamp) throws IOException {
        cbor.writeTag(CBOR_DATETIME_TAG);
        cbor.writeString(timestamp.toString());
    }

    @Override
    public void writeTimestampUTC(Date date) throws IOException {
        cbor.writeTag(CBOR_DATETIME_TAG);
        cbor.writeString(date.toString());
    }

    @Override
    public void writeSymbol(String s) throws IOException {
        cbor.writeString(s);
    }

    @Override
    public void writeSymbolToken(SymbolToken symbolToken) throws IOException {
        writeSymbol(symbolToken.assumeText());
    }

    @Override
    public void writeString(String s) throws IOException {
        cbor.writeString(s);
    }

    @Override
    public void writeClob(byte[] bytes) throws IOException {
        cbor.writeBinary(bytes);
    }

    @Override
    public void writeClob(byte[] bytes, int i, int i1) throws IOException {
        cbor.writeBinary(bytes, i, i1);
    }

    @Override
    public void writeBlob(byte[] bytes) throws IOException {
        cbor.writeBinary(bytes);
    }

    @Override
    public void writeBlob(byte[] bytes, int i, int i1) throws IOException {
        cbor.writeBinary(bytes, i, i1);
    }

    @Override
    public <T> T asFacet(Class<T> aClass) {
        throw new UnsupportedOperationException("No facets available.");
    }
}
