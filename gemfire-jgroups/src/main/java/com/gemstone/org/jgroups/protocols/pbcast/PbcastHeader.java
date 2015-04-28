/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PbcastHeader.java,v 1.3 2004/07/05 05:49:41 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;

import com.gemstone.org.jgroups.Header;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Hashtable;




public class PbcastHeader extends Header {
    public static final int MCAST_MSG  = 0;  // regular multicast message
    public static final int GOSSIP     = 1;  // gossip message (unicast)
    public static final int XMIT_REQ   = 2;  // retransmit request (unicast)
    public static final int XMIT_RSP   = 3;  // retransmit response (unicast)
    public static final int NOT_MEMBER = 4;  // shun message (unicast)



    int       type=-1;
    long      seqno=-1;        // we start out with 0 as first seqno for an mcast message
    Gossip    gossip=null;     // used to identify gossips, implements the equals() and hashCode() methods
    Hashtable xmit_reqs=null;  // for XMIT_REQs. keys=sender, vals=List of Longs (list of missing msgs)



    public PbcastHeader() {
	type=-1;
    }
    

    public PbcastHeader(int type) {
	this.type=type;
    }


    public PbcastHeader(int type, long seqno) {
	this.type=type; this.seqno=seqno;
    }


    public PbcastHeader(Gossip g, int type) {
	this.type=type;	gossip=g;
    }


    public PbcastHeader(Gossip g, int type, long seqno) {
	this.type=type; this.seqno=seqno;
	gossip=g;
    }

    

    public long getSeqno() {return seqno;}


    @Override // GemStoneAddition
    public String toString() {
	StringBuffer sb=new StringBuffer();
	sb.append("[PBCAST(" + type2String(type) + "), seqno=" + seqno);
	if(gossip != null) sb.append(", gossip=" + gossip);
	sb.append(']');
	return sb.toString();
    }


    @Override // GemStoneAddition
    public long size(short version) {
	return 500;
    }

    
    public static String type2String(int t) {
	switch(t) {
	case MCAST_MSG:  return "MCAST_MSG";
	case GOSSIP:     return "GOSSIP";
	case XMIT_REQ:   return "XMIT_REQ";
	case XMIT_RSP:   return "XMIT_RSP";
	case NOT_MEMBER: return "NOT_MEMBER";
	default:         return "UNKNOWN";
	}
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeInt(type);
	out.writeLong(seqno);
	out.writeObject(gossip);
	out.writeObject(xmit_reqs);
    }
	
	
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	type=in.readInt();
	seqno=in.readLong();
	gossip=(Gossip)in.readObject();
	xmit_reqs=(Hashtable)in.readObject();
    }


}
