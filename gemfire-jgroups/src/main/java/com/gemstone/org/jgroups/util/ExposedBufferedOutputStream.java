/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedBufferedOutputStream.java,v 1.1 2005/07/25 15:53:36 belaban Exp $
 */
public class ExposedBufferedOutputStream extends BufferedOutputStream {
    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream.
     *
     * @param out the underlying output stream.
     */
    public ExposedBufferedOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream with the specified buffer
     * size.
     *
     * @param out  the underlying output stream.
     * @param size the buffer size.
     * @throws IllegalArgumentException if size &lt;= 0.
     */
    public ExposedBufferedOutputStream(OutputStream out, int size) {
        super(out, size);
    }

    public void reset(int size) {
        count=0;
        if(size > buf.length) {
            buf=new byte[size];
        }
    }
}
