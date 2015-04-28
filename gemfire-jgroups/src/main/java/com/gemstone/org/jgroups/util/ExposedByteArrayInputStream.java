/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.ByteArrayInputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedByteArrayInputStream.java,v 1.1 2005/07/25 15:53:36 belaban Exp $
 */
public class ExposedByteArrayInputStream extends ByteArrayInputStream {

    
    /**
     * Creates a <code>ByteArrayInputStream</code>
     * so that it  uses <code>buf</code> as its
     * buffer array.
     * The buffer array is not copied.
     * The initial value of <code>pos</code>
     * is <code>0</code> and the initial value
     * of  <code>count</code> is the length of
     * <code>buf</code>.
     *
     * @param buf the input buffer.
     */
    public ExposedByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Creates <code>ByteArrayInputStream</code>
     * that uses <code>buf</code> as its
     * buffer array. The initial value of <code>pos</code>
     * is <code>offset</code> and the initial value
     * of <code>count</code> is the minimum of <code>offset+length</code>
     * and <code>buf.length</code>.
     * The buffer array is not copied. The buffer's mark is
     * set to the specified offset.
     *
     * @param buf    the input buffer.
     * @param offset the offset in the buffer of the first byte to read.
     * @param length the maximum number of bytes to read from the buffer.
     */
    public ExposedByteArrayInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }

    public void setData(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.count = Math.min(offset + length, buf.length);
        this.mark = offset;
    }


}
