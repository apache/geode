/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: StateTransferInfo.java,v 1.3 2004/09/16 13:55:32 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.Address;

import java.util.Vector;


/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. If type is GET_FROM_SINGLE, then the state is retrieved from 'target'. If
 * target is null, then the state will be retrieved from the oldest member (usually the
 * coordinator). If type is GET_FROM_MANY, the the state is retrieved from
 * 'targets'. If targets is null, then the state is retrieved from all members.
 *
 * @author Bela Ban
 */
public class StateTransferInfo {
    public static final int GET_FROM_SINGLE=1;  // get the state from a single member
    public static final int GET_FROM_MANY=2;    // get the state from multiple members (possibly all)

    public Address requester=null;
    public int     type=GET_FROM_SINGLE;
    public Address target=null;
    public Vector  targets=null;
    public long    timeout=0;


    public StateTransferInfo(Address requester, int type, Address target) {
        this.requester=requester;
        this.type=type;
        this.target=target;
    }

    public StateTransferInfo(int type, Address target) {
        this.type=type;
        this.target=target;
    }


    public StateTransferInfo(int type, Vector targets) {
        this.type=type;
        this.targets=targets;
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("type=" + (type == GET_FROM_MANY ? "GET_FROM_MANY" : "GET_FROM_SINGLE") + ", ");
        if(type == GET_FROM_MANY)
            ret.append("targets=" + targets);
        else
            ret.append("target=" + target);
        ret.append(", timeout=" + timeout);
        return ret.toString();
    }
}
