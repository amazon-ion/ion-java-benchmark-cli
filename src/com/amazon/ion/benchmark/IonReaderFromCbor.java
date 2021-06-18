package com.amazon.ion.benchmark;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

/**
 * Allows reading CBOR data via the IonReader interface. Useful for converting CBOR data to Ion.
 */
public class IonReaderFromCbor implements IonReader {

    private final CBORParser cbor;
    int tag = -1;
    private IonType type;
    private int depth = 0;

    IonReaderFromCbor(CBORParser cbor) {
        this.cbor = cbor;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("hasNext() not supported.");
    }

    @Override
    public IonType next() {
        JsonToken token;
        try {
            token = cbor.nextValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (token == null) {
            return null;
        }
        tag = cbor.getCurrentTag();
        switch(token) {
            case VALUE_NULL:
                type = IonType.NULL;
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
                type = IonType.BOOL;
                break;
            case VALUE_NUMBER_INT:
                type = IonType.INT;
                break;
            case VALUE_NUMBER_FLOAT:
                JsonParser.NumberType numberType;
                try {
                    numberType = cbor.getNumberType();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (numberType == JsonParser.NumberType.BIG_DECIMAL) {
                    type = IonType.DECIMAL;
                } else {
                    type = IonType.FLOAT;
                }
                break;
            case VALUE_STRING:
                if (tag == 0) {
                    type = IonType.TIMESTAMP;
                } else {
                    type = IonType.STRING;
                }
                break;
            case VALUE_EMBEDDED_OBJECT:
                type = IonType.BLOB;
                break;
            case START_ARRAY:
                type = IonType.LIST;
                break;
            case START_OBJECT:
                type = IonType.STRUCT;
                break;
            case END_ARRAY:
            case END_OBJECT:
                type = null;
                break;
            default:
                type = null;
                break;
        }
        return type;
    }

    @Override
    public void stepIn() {
        if (!IonType.isContainer(type)) {
            throw new IllegalStateException("Cannot step into a scalar.");
        }
        depth++;
        // Do nothing. CBORParser automatically steps into containers.
    }

    @Override
    public void stepOut() {
        if (type != null) {
            try {
                cbor.skipChildren();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            depth--;
        }
    }

    @Override
    public int getDepth() {
        return depth;
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException("CBOR does not use symbol tables.");
    }

    @Override
    public IonType getType() {
        return type;
    }

    @Override
    public IntegerSize getIntegerSize() {
        try {
            switch (cbor.getNumberType()) {
                case INT:
                    return IntegerSize.INT;
                case LONG:
                    return IntegerSize.LONG;
                case BIG_INTEGER:
                    return IntegerSize.BIG_INTEGER;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private static final String[] NO_ANNOTATIONS = new String[0];
    private static final SymbolToken[] NO_ANNOTATION_SYMBOLS = new SymbolToken[0];

    @Override
    public String[] getTypeAnnotations() {
        return NO_ANNOTATIONS;
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return NO_ANNOTATION_SYMBOLS;
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        return null;
    }

    @Override
    public int getFieldId() {
        throw new UnsupportedOperationException("CBOR does not have symbol IDs.");
    }

    @Override
    public String getFieldName() {
        try {
            return cbor.getCurrentName();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return new SymbolToken() {
            @Override
            public String getText() {
                return getFieldName();
            }

            @Override
            public String assumeText() {
                return getFieldName();
            }

            @Override
            public int getSid() {
                return -1;
            }
        };
    }

    @Override
    public boolean isNullValue() {
        return type == IonType.NULL;
    }

    @Override
    public boolean isInStruct() {
        try {
            return cbor.currentName() != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean booleanValue() {
        try {
            return cbor.getBooleanValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int intValue() {
        try {
            return cbor.getIntValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long longValue() {
        try {
            return cbor.getLongValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BigInteger bigIntegerValue() {
        try {
            return cbor.getBigIntegerValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double doubleValue() {
        try {
            return cbor.getDoubleValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BigDecimal bigDecimalValue() {
        try {
            return cbor.getDecimalValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Decimal decimalValue() {
        return Decimal.valueOf(bigDecimalValue());
    }

    @Override
    public Date dateValue() {
        return timestampValue().dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        if (tag != 0) {
            throw new IllegalStateException("CBOR datetime values use tag 0.");
        }
        return Timestamp.valueOf(stringValue());
    }

    @Override
    public String stringValue() {
        try {
            return cbor.getValueAsString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SymbolToken symbolValue() {
        throw new IllegalStateException("CBOR does not have a symbol type.");
    }

    @Override
    public int byteSize() {
        try {
            return cbor.getBinaryValue().length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] newBytes() {
        try {
            return cbor.getBinaryValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getBytes(byte[] bytes, int i, int i1) {
        byte[] value = newBytes();
        System.arraycopy(value, 0, bytes, i, i1);
        return i1;
    }

    @Override
    public <T> T asFacet(Class<T> aClass) {
        throw new UnsupportedOperationException("No facets available.");
    }

    @Override
    public void close() throws IOException {
        cbor.close();
    }
}
