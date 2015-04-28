/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

/**
 * This exception indicated that lock manager refused to give a lock on 
 * some resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LockNotGrantedException extends Exception  {
private static final long serialVersionUID = 2196485072005850175L;

    public LockNotGrantedException() {
        super();
    }

    public LockNotGrantedException(String s) {
        super(s);
    }
    
}
