/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Digest.java,v 1.4 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;




/**
 * Message digest, collecting the highest sequence number seen so far for each member, plus the
 * messages that have higher seqnos than the ones given.
 */
public class Digest implements Serializable {
    public long[]      highest_seqnos=null; // highest seqno received for each member
    public final List  msgs=new List();     // msgs (for each member) whose seqnos are higher than the
    private static final long serialVersionUID = -1750370226005341630L;
    // ones sent by the FLUSH coordinator

    public Digest(int size) {
        highest_seqnos=new long[size];
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer retval=new StringBuffer();
        retval.append(Util.array2String(highest_seqnos)).append(" (").append(msgs.size()).append(" msgs)");
        return retval.toString();
    }
    
}
