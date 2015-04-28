/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TimeoutException.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

import java.util.List;


/**
 * Thrown if members fail to respond in time.
 */
public class TimeoutException extends Exception {
private static final long serialVersionUID = 4078270477623063306L;
    List failed_mbrs=null; // members that failed responding

    public TimeoutException() {
        super("TimeoutException");
    }

    public TimeoutException(String msg) {
        super(msg);
    }

    public TimeoutException(List failed_mbrs) {
        super("TimeoutException");
        this.failed_mbrs=failed_mbrs;
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();

        sb.append(super.toString());

        if(failed_mbrs != null && failed_mbrs.size() > 0)
            sb.append(" (failed members: ").append(failed_mbrs);
        return sb.toString();
    }
}
