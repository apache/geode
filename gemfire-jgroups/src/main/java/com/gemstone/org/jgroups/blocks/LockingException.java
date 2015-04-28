/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: LockingException.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $


package com.gemstone.org.jgroups.blocks;

import java.util.Map;


public class LockingException extends Exception {
private static final long serialVersionUID = 591671983577853564L;
    Map failed_lockers=null; // list of members who failed acquiring locks (keys=Address, values=exception string)

    public LockingException(String msg) {
	super(msg);
    }

    public LockingException(Map m) {
        super("LockingException");
        failed_lockers=m;
    }

    
    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();

        sb.append(super.toString());

        if(failed_lockers != null && failed_lockers.size() > 0)
            sb.append(" (failed members: ").append(failed_lockers);
        return sb.toString();
    }
    
}
