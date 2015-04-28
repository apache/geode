/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Header.java,v 1.9 2005/08/08 09:47:19 belaban Exp $

package com.gemstone.org.jgroups;

import java.io.Externalizable;



/**
 Abstract base class for all headers to be added to a Message.
 @author Bela Ban
 */
public abstract class Header implements Externalizable {
    public static final long HDR_OVERHEAD=100; // estimated size of a header (used to estimate the size of the entire msg)


    public Header() {

    }


    /**
     * To be implemented by subclasses. Return the size of this object for the serialized version of it.
     * I.e. how many bytes this object takes when flattened into a buffer. This may be different for each instance,
     * or can be the same. This may also just be an estimation. E.g. FRAG uses it on Message to determine whether
     * or not to fragment the message. Fragmentation itself will be accurate, because the entire message will actually
     * be serialized into a byte buffer, so we can determine the exact size.
     */
    public long size(short version) {
        return HDR_OVERHEAD;
    }

//    public void writeTo(DataOutputStream out) throws IOException {
//        ;
//    }
//
//    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
//        ;
//    }


    @Override // GemStoneAddition
    public String toString() {
        return '[' + getClass().getName() + " Header]";
    }

}
