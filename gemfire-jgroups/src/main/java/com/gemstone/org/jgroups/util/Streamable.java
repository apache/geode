/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementations of Streamable can add their state directly to the output stream, enabling them to bypass costly
 * serialization
 * @author Bela Ban
 * @version $Id: Streamable.java,v 1.2 2005/07/25 16:21:47 belaban Exp $
 */
public interface Streamable {

    /** Write the entire state of the current object (including superclasses) to outstream.
     * Note that the output stream <em>must not</em> be closed */
    void writeTo(DataOutputStream out) throws IOException;

    /** Read the state of the current object (including superclasses) from instream
     * Note that the input stream <em>must not</em> be closed */
    void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException;
}
