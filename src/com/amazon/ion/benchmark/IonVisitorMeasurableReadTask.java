package com.amazon.ion.benchmark;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.v3.AnnotationIterator;
import com.amazon.ion.v3.StreamReaderAsIonReader;
import com.amazon.ion.v3.impl_1_0.StreamReader_1_0;
import com.amazon.ion.v3.impl_1_1.StreamReaderImpl;
import com.amazon.ion.v3.visitor.VisitingReaderCallback;
import com.amazon.ion.v3.visitor.VisitingReaderCallbackBase;
import com.amazon.ion.v3.StreamReader;
import com.amazon.ion.v3.visitor.ApplicationReaderDriver;
import com.amazon.ion.v3.TokenType;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

/**
 * A MeasurableReadTask for reading data in the Ion format (either text or binary).
 */
class IonVisitorMeasurableReadTask extends MeasurableReadTask {

    private SideEffectConsumerVisitor consumerVisitor = null;

    private static class SideEffectConsumerVisitor extends VisitingReaderCallbackBase {
        private final SideEffectConsumer consumer;

        public SideEffectConsumerVisitor(SideEffectConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public VisitingReaderCallback onAnnotation(AnnotationIterator iterator) {
            while (iterator.hasNext()) {
                iterator.next();
                consumer.consume(iterator.getText());
            }
            return this;
        }

        @Override
        public VisitingReaderCallback onField(String s, int i) {
            consumer.consume(s);
            return this;
        }

        @Override
        public void onListStart() {
        }

        @Override
        public void onListEnd() {
        }

        @Override
        public void onSexpStart() {
        }

        @Override
        public void onSexpEnd() {
        }

        @Override
        public void onStructStart() {
        }

        @Override
        public void onStructEnd() {

        }

        @Override
        public VisitingReaderCallback onValue(TokenType type) {
            consumer.consume(type);
            return this;
        }

        @Override
        public void onNull(IonType ionType) {
            consumer.consume(ionType);
        }

        @Override
        public void onBoolean(boolean b) {
            consumer.consume(b);
        }

        @Override
        public void onLongInt(long l) {
            consumer.consume(l);
        }

        @Override
        public void onBigInt(BigInteger bigInteger) {
            consumer.consume(bigInteger);
        }

        @Override
        public void onFloat(double v) {
            consumer.consume(v);
        }

        @Override
        public void onDecimal(Decimal decimal) {
            consumer.consume(decimal);
        }

        @Override
        public void onTimestamp(Timestamp timestamp) {
            consumer.consume(timestamp);
        }

        @Override
        public void onString(String s) {
            consumer.consume(s);
        }

        @Override
        public void onSymbol(String s, int sid) {
            consumer.consume(s);
        }

        @Override
        public void onClob(ByteBuffer byteBuffer) {
            consumer.consume(byteBuffer);
        }

        @Override
        public void onBlob(ByteBuffer byteBuffer) {
            consumer.consume(byteBuffer);
        }
    }

    /**
     * @param inputPath the Ion data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    IonVisitorMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
    }

    @Override
    public void setUpTrial() throws IOException {
        super.setUpTrial();
    }

    @Override
    public void setUpIteration() {
        // Nothing to do.
    }

    @Override
    public void tearDownIteration() {
        // Nothing to do.
    }

    @Override
    void fullyTraverseFromBuffer(SideEffectConsumer consumer) throws IOException {
        this.consumerVisitor = new SideEffectConsumerVisitor(consumer);
        ApplicationReaderDriver driver = new ApplicationReaderDriver(ByteBuffer.wrap(buffer));
        driver.readAll(consumerVisitor);
        try {
            driver.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(inputFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            this.consumerVisitor = new SideEffectConsumerVisitor(consumer);
            ApplicationReaderDriver driver = new ApplicationReaderDriver(mappedByteBuffer);
            driver.readAll(consumerVisitor);
            try {
                driver.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    void traverseFromBuffer(List<String> paths, SideEffectConsumer consumer) throws IOException {
        throw new UnsupportedOperationException("traverseFromBuffer");
    }

    @Override
    public void traverseFromFile(List<String> paths, SideEffectConsumer consumer) throws IOException {
        throw new UnsupportedOperationException("traverseFromFile");
    }

    @Override
    public void fullyReadDomFromBuffer(SideEffectConsumer consumer) throws IOException {
        throw new UnsupportedOperationException("fullyReadDomFromBuffer");
    }

    @Override
    public void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException {
        throw new UnsupportedOperationException("fullyReadDomFromFile");
    }
}
