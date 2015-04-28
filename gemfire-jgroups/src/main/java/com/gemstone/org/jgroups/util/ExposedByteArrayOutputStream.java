/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.ByteArrayOutputStream;

/**
 * Extends ByteArrayOutputStream, but exposes the internal buffer. This way we don't need to call
 * toByteArray() which copies the internal buffer
 * @author Bela Ban
 * @version $Id: ExposedByteArrayOutputStream.java,v 1.2 2005/07/25 15:53:36 belaban Exp $
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getRawBuffer() {
        return buf;
    }

    public int getCapacity() {
        return buf.length;
    }
}
