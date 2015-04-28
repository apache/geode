/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.util.GemFireTracer;


import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedBufferedInputStream.java,v 1.3 2005/07/25 16:57:31 belaban Exp $
 */
public class ExposedBufferedInputStream extends BufferedInputStream {
    private final static GemFireTracer log=GemFireTracer.getLog(ExposedBufferedInputStream.class);
    /**
     * Creates a <code>BufferedInputStream</code>
     * and saves its  argument, the input stream
     * <code>in</code>, for later use. An internal
     * buffer array is created and  stored in <code>buf</code>.
     *
     * @param in the underlying input stream.
     */
    public ExposedBufferedInputStream(InputStream in) {
        super(in);
    }

    /**
     * Creates a <code>BufferedInputStream</code>
     * with the specified buffer size,
     * and saves its  argument, the input stream
     * <code>in</code>, for later use.  An internal
     * buffer array of length  <code>size</code>
     * is created and stored in <code>buf</code>.
     *
     * @param in   the underlying input stream.
     * @param size the buffer size.
     * @throws IllegalArgumentException if size <= 0.
     */
    public ExposedBufferedInputStream(InputStream in, int size) {
        super(in, size);
    }

    public void reset(int size) {
        count=pos=marklimit=0;
        markpos=-1;
        if(buf != null) {
            if(size > buf.length) {
                buf=new byte[size];
            }
        }
        else {
            buf=new byte[4096];
            if(log.isWarnEnabled())
                log.warn("output stream was closed, re-creating it (please don't close it)");
        }
    }
}
