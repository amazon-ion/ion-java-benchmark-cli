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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazon.ionelement.api.Ion.ionBool;
import static com.amazon.ionelement.api.Ion.ionDecimal;
import static com.amazon.ionelement.api.Ion.ionFloat;
import static com.amazon.ionelement.api.Ion.ionInt;
import static com.amazon.ionelement.api.Ion.ionListOf;
import static com.amazon.ionelement.api.Ion.ionNull;
import static com.amazon.ionelement.api.Ion.ionTimestamp;
import static com.amazon.ionelement.api.Ion.ionSexpOf;
import static com.amazon.ionelement.api.Ion.ionString;
import static com.amazon.ionelement.api.Ion.ionStructOf;
import static com.amazon.ionelement.api.Ion.ionSymbol;

public class SingleIonElementBuilder implements IonVisitor {

    public IonElement element = null;

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

    public void onNull(AnnotationIterator annotationIterator, IonType type) {
        element = (ionNull(ElementType.NULL, handleAnnotations(annotationIterator)));
    }

    @Override
    public void onBool(AnnotationIterator annotationIterator, boolean b) {
        element = (ionBool(b, handleAnnotations(annotationIterator)));

    }

    @Override
    public void onLong(AnnotationIterator annotationIterator, long l) {
        element = (ionInt(l, handleAnnotations(annotationIterator)));
    }

    @Override
    public void onBigInt(AnnotationIterator annotationIterator, Function0<? extends BigInteger> function0) {
        // TODO
    }

    @Override
    public void onDouble(AnnotationIterator annotationIterator, double v) {
        element = (ionFloat(v, handleAnnotations(annotationIterator)));
    }

    @Override
    public void onDecimal(AnnotationIterator annotationIterator, Function0<? extends BigDecimal> function0) {
        element = (ionDecimal(Decimal.valueOf(function0.invoke()), handleAnnotations(annotationIterator)));
    }

    @Override
    public void onTimestamp(AnnotationIterator annotationIterator, Function0<Timestamp> function0) {
        element = (ionTimestamp(function0.invoke(), handleAnnotations(annotationIterator)));
    }

    @Override
    public void onSymbol(AnnotationIterator annotationIterator, int i, Function0<String> function0) {
        element = (ionSymbol(function0.invoke(), handleAnnotations(annotationIterator)));
    }

    @Override
    public void onString(AnnotationIterator annotationIterator, Function0<String> function0) {
        element = (ionString(function0.invoke(), handleAnnotations(annotationIterator)));
    }

    @Override
    public void onBlob(AnnotationIterator annotationIterator, ByteBuffer byteBuffer) {

    }

    @Override
    public void onClob(AnnotationIterator annotationIterator, ByteBuffer byteBuffer) {

    }

    @Override
    public void onList(AnnotationIterator annotationIterator, Function1<? super IonVisitor, Unit> function1) {
        IonElementBuilder children = new IonElementBuilder();
        function1.invoke(children);
        element = (ionListOf(children.elements, handleAnnotations(annotationIterator)));
    }

    @Override
    public void onSexp(AnnotationIterator annotationIterator, Function1<? super IonVisitor, Unit> function1) {
        IonElementBuilder children = new IonElementBuilder();
        function1.invoke(children);
        element = (ionSexpOf(children.elements, handleAnnotations(annotationIterator)));
    }

    @Override
    public void onStruct(AnnotationIterator annotationIterator, Function1<? super IonFieldVisitor, Unit> function1) {
        IonFieldBuilder children = new IonFieldBuilder();
        function1.invoke(children);
        element = (ionStructOf(children.elements, handleAnnotations(annotationIterator)));
    }

    public void onMacro(MacroV2 macro, Function1<? super IonVisitor, Unit> evaluationVisitor, Function1<? super IonVisitor, Unit> argVisitor) {}
}
