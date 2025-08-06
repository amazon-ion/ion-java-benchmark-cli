package com.amazon.ion.benchmark;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.shaded_.do_not_use.kotlin.Unit;
import com.amazon.ion.shaded_.do_not_use.kotlin.jvm.functions.Function0;
import com.amazon.ion.shaded_.do_not_use.kotlin.jvm.functions.Function1;
import com.amazon.ion.v3.impl_1_1.MacroV2;
import com.amazon.ion.v3.visitor2.AnnotationIterator;
import com.amazon.ion.v3.visitor2.IonFieldVisitor;
import com.amazon.ion.v3.visitor2.IonVisitor;
import com.amazon.ionelement.api.ElementType;
import com.amazon.ionelement.api.IonElement;
import com.amazon.ionelement.api.StructField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazon.ionelement.api.Ion.field;
import static com.amazon.ionelement.api.Ion.ionBool;
import static com.amazon.ionelement.api.Ion.ionDecimal;
import static com.amazon.ionelement.api.Ion.ionFloat;
import static com.amazon.ionelement.api.Ion.ionInt;
import static com.amazon.ionelement.api.Ion.ionListOf;
import static com.amazon.ionelement.api.Ion.ionNull;
import static com.amazon.ionelement.api.Ion.ionSexpOf;
import static com.amazon.ionelement.api.Ion.ionString;
import static com.amazon.ionelement.api.Ion.ionStructOf;
import static com.amazon.ionelement.api.Ion.ionSymbol;
import static com.amazon.ionelement.api.Ion.ionTimestamp;

public class IonFieldBuilder implements IonFieldVisitor {

    final List<StructField> elements = new ArrayList(32);
    
    private List<String> handleAnnotations(AnnotationIterator annotations) {
        List<String> a;
        if (annotations == null) {
            a = Collections.emptyList();
        } else {
            a = new ArrayList(2);
            while (annotations.hasNext()) {
                a.add(annotations.next());
            }
        }
        return a;
    }
    
    public void onNull(String fieldName, AnnotationIterator annotationIterator, IonType type) {
        elements.add(field(fieldName, ionNull(ElementType.NULL, handleAnnotations(annotationIterator))));
    }
    
    @Override
    public void onBool(String fieldName, AnnotationIterator annotationIterator, boolean b) {
        elements.add(field(fieldName, ionBool(b, handleAnnotations(annotationIterator))));
    }

    @Override
    public void onLong(String fieldName, AnnotationIterator annotationIterator, long l) {
        elements.add(field(fieldName, ionInt(l, handleAnnotations(annotationIterator))));
    }

    @Override
    public void onBigInt(String fieldName, AnnotationIterator annotationIterator, Function0<? extends BigInteger> function0) {
        // TODO
    }

    @Override
    public void onDouble(String fieldName, AnnotationIterator annotationIterator, double v) {
        elements.add(field(fieldName, ionFloat(v, handleAnnotations(annotationIterator))));
    }

    @Override
    public void onDecimal(String fieldName, AnnotationIterator annotationIterator, Function0<? extends BigDecimal> function0) {
        elements.add(field(fieldName, ionDecimal(Decimal.valueOf(function0.invoke()), handleAnnotations(annotationIterator))));
    }


    @Override
    public void onTimestamp(String fieldName, AnnotationIterator annotationIterator, Function0<Timestamp> function0) {
        elements.add(field(fieldName, ionTimestamp(function0.invoke(), handleAnnotations(annotationIterator))));
    }

    @Override
    public void onSymbol(String fieldName, AnnotationIterator annotationIterator, int i, Function0<String> function0) {
        elements.add(field(fieldName, ionSymbol(function0.invoke(), handleAnnotations(annotationIterator))));
    }

    @Override
    public void onString(String fieldName, AnnotationIterator annotationIterator, Function0<String> function0) {
        elements.add(field(fieldName, ionString(function0.invoke(), handleAnnotations(annotationIterator))));
    }

    @Override
    public void onBlob(String fieldName, AnnotationIterator annotationIterator, ByteBuffer byteBuffer) {
        
    }

    @Override
    public void onClob(String fieldName, AnnotationIterator annotationIterator, ByteBuffer byteBuffer) {

    }

    @Override
    public void onList(String fieldName, AnnotationIterator annotationIterator, Function1<? super IonVisitor, Unit> function1) {
        IonElementBuilder children = new IonElementBuilder();
        function1.invoke(children);
        elements.add(field(fieldName, ionListOf(children.elements, handleAnnotations(annotationIterator))));
    }

    @Override
    public void onSexp(String fieldName, AnnotationIterator annotationIterator, Function1<? super IonVisitor, Unit> function1) {
        IonElementBuilder children = new IonElementBuilder();
        function1.invoke(children);
        elements.add(field(fieldName, ionSexpOf(children.elements, handleAnnotations(annotationIterator))));
    }

    @Override
    public void onStruct(String fieldName, AnnotationIterator annotationIterator, Function1<? super IonFieldVisitor, Unit> function1) {
        IonFieldBuilder children = new IonFieldBuilder();
        function1.invoke(children);
        elements.add(field(fieldName, ionStructOf(children.elements, handleAnnotations(annotationIterator))));
    }

    public void onMacro(String fieldName, MacroV2 macro, Function1<? super IonVisitor, Unit> evaluationVisitor, Function1<? super IonVisitor, Unit> argVisitor) {}
}
