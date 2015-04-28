/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedDataOutputStream.java,v 1.1 2005/07/25 15:53:37 belaban Exp $
 */
public class ExposedDataOutputStream extends DataOutputStream {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see java.io.FilterOutputStream#out
     */
    public ExposedDataOutputStream(OutputStream out) {
        super(out);
    }

    public void reset() {
        written=0;
    }
}
