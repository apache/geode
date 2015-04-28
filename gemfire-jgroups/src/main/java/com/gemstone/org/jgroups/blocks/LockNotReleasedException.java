/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

/**
 * This exception indicated that lock manager refused to release a lock on 
 * some resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LockNotReleasedException extends Exception {
private static final long serialVersionUID = -9174045093160454258L;

    public LockNotReleasedException() {
        super();
    }

    public LockNotReleasedException(String s) {
        super(s);
    }
    
}
