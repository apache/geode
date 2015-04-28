/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: NakAckHeader.java,v 1.4 2004/07/05 14:17:15 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.ViewId;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;



public class NakAckHeader extends Header {
    public static final int NAK_MSG          = 1;  // asynchronous msg
    public static final int NAK_ACK_MSG      = 2;  // synchronous msg
    public static final int WRAPPED_MSG      = 3;  // a wrapped msg, needs to be ACKed
    public static final int RETRANSMIT_MSG   = 4;  // retransmit msg
    public static final int NAK_ACK_RSP      = 5;  // ack to NAK_ACK_MSG, seqno contains ACKed
    public static final int OUT_OF_BAND_MSG  = 6;  // out-of-band msg
    public static final int OUT_OF_BAND_RSP  = 7;  // ack for out-of-band msg

    int     type=0;
    long    seqno=-1;          // either reg. NAK_ACK_MSG or first_seqno in retransmissions
    long    last_seqno=-1;     // used for retransmissions
    ViewId  vid=null;

    // the messages from this sender can be deleted safely (OutOfBander). Contains seqnos (Longs)
    Vector  stable_msgs=null;
    Address sender=null;       // In case of WRAPPED_MSG: the address to which an ACK has to be sent



    public NakAckHeader() {}
    


    public NakAckHeader(int type, long seqno, ViewId vid) {
	this.type=type;
	this.seqno=seqno;
	this.vid=vid;
    }


    @Override // GemStoneAddition
    public long size(short version) {
	return 512;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeInt(type);
	out.writeLong(seqno);
	out.writeLong(last_seqno);
	out.writeObject(vid);
	out.writeObject(stable_msgs);
	out.writeObject(sender);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	type=in.readInt();
	seqno=in.readLong();
	last_seqno=in.readLong();
	vid=(ViewId)in.readObject();
	stable_msgs=(Vector)in.readObject();
	sender=(Address)in.readObject();
    }


    public NakAckHeader copy() {
	NakAckHeader ret=new NakAckHeader(type, seqno, vid);
	ret.last_seqno=last_seqno;
	ret.stable_msgs=stable_msgs != null ? (Vector)stable_msgs.clone() : null;
	ret.sender=sender;
	return ret;
    }


    public static String type2Str(int t) {
	switch(t) {
	case NAK_MSG:          return "NAK_MSG";
	case NAK_ACK_MSG:      return "NAK_ACK_MSG";
	case WRAPPED_MSG:      return "WRAPPED_MSG";
	case RETRANSMIT_MSG:   return "RETRANSMIT_MSG";
	case NAK_ACK_RSP:      return "NAK_ACK_RSP";
	case OUT_OF_BAND_MSG:  return "OUT_OF_BAND_MSG";
	case OUT_OF_BAND_RSP:  return "OUT_OF_BAND_RSP";
	default:               return "<undefined>";
	}
    }

    @Override // GemStoneAddition
    public String toString() {
	StringBuffer ret=new StringBuffer();
	ret.append("[NAKACK: " + type2Str(type) + ", seqno=" + seqno + ", last_seqno=" + last_seqno +
		   ", vid=" + vid );
	if(type == WRAPPED_MSG)
	    ret.append(", sender=" + sender);
	ret.append(']');

	return ret.toString();
    }

}
