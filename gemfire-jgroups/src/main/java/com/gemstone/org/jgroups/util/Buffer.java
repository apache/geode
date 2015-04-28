/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

/**
 * Buffer with an offset and length. Will be replaced with NIO equivalent once JDK 1.4 becomes baseline
 * @author Bela Ban
 * @version $Id: Buffer.java,v 1.4 2005/09/06 09:53:53 belaban Exp $
 */
public class Buffer {
    byte[] buf;
    int offset;
    int length;

    public Buffer(byte[] buf, int offset, int length) {
        this.buf=buf;
        this.offset=offset;
        this.length=length;
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] buf) {
        this.buf=buf;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset=offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length=length;
    }

    public Buffer copy() {
        byte[] new_buf=buf != null? new byte[length] : null;
        int new_length=new_buf != null? new_buf.length : 0;
        if(new_buf != null)
            System.arraycopy(buf, offset, new_buf, 0, length);
        return new Buffer(new_buf, 0, new_length);
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append(length).append(" bytes");
        if(offset > 0)
            sb.append(" (offset=").append(offset).append(")");
        return sb.toString();
    }

}
