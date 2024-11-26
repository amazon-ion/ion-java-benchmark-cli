package com.amazon.ion.benchmark;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.system.IonSystemBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Records MacroAwareIonWriter method calls as WriteInstructions that may be replayed later.
 */
class RecordingMacroAwareIonWriter implements MacroAwareIonWriter {

    private final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private final Consumer<MeasurableWriteTask.WriteInstruction<MacroAwareIonWriter>> instructionsSink;

    /**
     * @param instructionsSink sink for recorded instructions.
     */
    RecordingMacroAwareIonWriter(Consumer<MeasurableWriteTask.WriteInstruction<MacroAwareIonWriter>> instructionsSink) {
        this.instructionsSink = instructionsSink;
    }

    @Override
    public void startEncodingSegmentWithIonVersionMarker() {
        instructionsSink.accept(MacroAwareIonWriter::startEncodingSegmentWithIonVersionMarker);
    }

    @Override
    public void startEncodingSegmentWithEncodingDirective(
        Map<MacroRef, ? extends Macro> newMacros,
        boolean isMacroTableAppend,
        List<String> newSymbols,
        boolean isSymbolTableAppend,
        boolean encodingDirectiveAlreadyWritten
    ) {
        // Note: the collections passed in may be mutated after this method returns. Therefore, we copy them so that
        // when replayed they do not have different contents.
        Map<MacroRef, ? extends Macro> newMacrosCopy = new LinkedHashMap<>(newMacros);
        List<String> listCopy = new ArrayList<>(newSymbols);
        instructionsSink.accept(
            w -> w.startEncodingSegmentWithEncodingDirective(newMacrosCopy, isMacroTableAppend, listCopy, isSymbolTableAppend, encodingDirectiveAlreadyWritten)
        );
    }

    @Override
    public void startMacro(Macro macro) {
        instructionsSink.accept(w -> w.startMacro(macro));
    }

    @Override
    public void startMacro(String s, Macro macro) {
        instructionsSink.accept(w -> w.startMacro(s, macro));
    }

    @Override
    public void endMacro() {
        instructionsSink.accept(MacroAwareIonWriter::endMacro);
    }

    @Override
    public void startExpressionGroup() {
        instructionsSink.accept(MacroAwareIonWriter::startExpressionGroup);
    }

    @Override
    public void endExpressionGroup() {
        instructionsSink.accept(MacroAwareIonWriter::endExpressionGroup);
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException("getSymbolTable() should not be called during benchmarking.");
    }

    @Override
    public void flush() {
        instructionsSink.accept(MacroAwareIonWriter::flush);
    }

    @Override
    public void finish() {
        instructionsSink.accept(MacroAwareIonWriter::finish);
    }

    @Override
    public void close() {
        instructionsSink.accept(MacroAwareIonWriter::close);
    }

    @Override
    public void setFieldName(String name) {
        instructionsSink.accept(w -> w.setFieldName(name));
    }

    @Override
    public void setFieldNameSymbol(SymbolToken name) {
        instructionsSink.accept(w -> w.setFieldNameSymbol(name));
    }

    @Override
    public void setTypeAnnotations(String... annotations) {
        instructionsSink.accept(w -> w.setTypeAnnotations(annotations));
    }

    @Override
    public void setTypeAnnotationSymbols(SymbolToken... annotations) {
        instructionsSink.accept(w -> w.setTypeAnnotationSymbols(annotations));
    }

    @Override
    public void addTypeAnnotation(String annotation) {
        instructionsSink.accept(w -> w.addTypeAnnotation(annotation));
    }

    @Override
    public void stepIn(IonType containerType) {
        instructionsSink.accept(w-> w.stepIn(containerType));
    }

    @Override
    public void stepOut() {
        instructionsSink.accept(MacroAwareIonWriter::stepOut);
    }

    @Override
    public boolean isInStruct() {
        throw new UnsupportedOperationException("isInStruct() should not be called during benchmarking.");
    }

    @Override
    public void writeValue(IonValue value) {
        throw new UnsupportedOperationException("writeValue(IonValue) should not be called during benchmarking.");
    }

    @Override
    public void writeValue(IonReader reader) {
        // Note: w -> w.writeValue(reader) is not correct because it does not capture the value at which the reader
        // is currently positioned. For now, we use IonValue to achieve this, though it would be more efficient to
        // capture a primitive and store an instruction that writes that primitive to the writer directly.
        IonValue value = SYSTEM.newValue(reader);
        instructionsSink.accept(value::writeTo);
    }

    @Override
    public void writeValues(IonReader reader) {
        throw new UnsupportedOperationException("writeValues() should not be called during benchmarking.");
    }

    @Override
    public void writeNull() {
        instructionsSink.accept(MacroAwareIonWriter::writeNull);
    }

    @Override
    public void writeNull(IonType type) {
        instructionsSink.accept(w -> w.writeNull(type));
    }

    @Override
    public void writeBool(boolean value) {
        instructionsSink.accept(w -> w.writeBool(value));
    }

    @Override
    public void writeInt(long value) {
        instructionsSink.accept(w -> w.writeInt(value));
    }

    @Override
    public void writeInt(BigInteger value) {
        instructionsSink.accept(w -> w.writeInt(value));
    }

    @Override
    public void writeFloat(double value) {
        instructionsSink.accept(w -> w.writeFloat(value));
    }

    @Override
    public void writeDecimal(BigDecimal value) {
        instructionsSink.accept(w -> w.writeDecimal(value));
    }

    @Override
    public void writeTimestamp(Timestamp value) {
        instructionsSink.accept(w -> w.writeTimestamp(value));
    }

    @Override
    public void writeTimestampUTC(Date value) {
        instructionsSink.accept(w -> w.writeTimestampUTC(value));
    }

    @Override
    public void writeSymbol(String content) {
        instructionsSink.accept(w -> w.writeSymbol(content));
    }

    @Override
    public void writeSymbolToken(SymbolToken content) {
        instructionsSink.accept(w -> w.writeSymbolToken(content));
    }

    @Override
    public void writeString(String value) {
        instructionsSink.accept(w -> w.writeString(value));
    }

    @Override
    public void writeClob(byte[] value) {
        instructionsSink.accept(w -> w.writeClob(value));
    }

    @Override
    public void writeClob(byte[] value, int start, int len) {
        instructionsSink.accept(w -> w.writeClob(value, start, len));
    }

    @Override
    public void writeBlob(byte[] value) {
        instructionsSink.accept(w -> w.writeBlob(value));
    }

    @Override
    public void writeBlob(byte[] value, int start, int len) {
        instructionsSink.accept(w -> w.writeBlob(value, start, len));
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        throw new UnsupportedOperationException("asFacet() should not be called during benchmarking.");
    }
}
