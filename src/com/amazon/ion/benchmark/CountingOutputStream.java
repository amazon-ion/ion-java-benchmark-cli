package com.amazon.ion.benchmark;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CountingOutputStream extends FilterOutputStream {
    private long count;

    /**
     * Creates an output stream filter built on top of the specified
     * underlying output stream.
     *
     * @param out the underlying output stream to be assigned to
     *            the field <tt>this.out</tt> for later use, or
     *            <code>null</code> if this instance is to be
     *            created without an underlying stream.
     */
    public CountingOutputStream(OutputStream out) {
        super(out);
    }

    /** Returns the number of bytes written. */
    public long getCount() {
        return count;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        this.count += len;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        this.count++;
    }
}
