/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: NakAckHeader.java,v 1.16 2005/08/17 11:05:28 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.util.Range;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;


public class NakAckHeader extends Header implements Streamable {
    public static final byte MSG=1;       // regular msg
    public static final byte XMIT_REQ=2;  // retransmit request
    public static final byte XMIT_RSP=3;  // retransmit response (contains one or more messages)


    byte  type=0;
    long  seqno=-1;        // seqno of regular message (MSG)
    Range range=null;      // range of msgs to be retransmitted (XMIT_REQ) or retransmitted (XMIT_RSP)
    Address sender;        // the original sender of the message (for XMIT_REQ)


    public NakAckHeader() {
    }


    /**
     * Constructor for regular messages
     */
    public NakAckHeader(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }

    /**
     * Constructor for retransmit requests/responses (low and high define the range of msgs)
     */
    public NakAckHeader(byte type, long low, long high) {
        this.type=type;
        range=new Range(low, high);
    }


    public NakAckHeader(byte type, long low, long high, Address sender) {
        this(type, low, high);
        this.sender=sender;
    }




    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(seqno);
        if(range != null) {
            out.writeBoolean(true);  // wasn't here before, bad bug !
            range.writeExternal(out);
        }
        else
            out.writeBoolean(false);
        out.writeObject(sender);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean read_range;
        type=in.readByte();
        seqno=in.readLong();
        read_range=in.readBoolean();
        if(read_range) {
            range=new Range();
            range.readExternal(in);
        }
        sender=(Address)in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        out.writeLong(seqno);
        Util.writeStreamable(range, out);
        Util.writeAddress(sender, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        seqno=in.readLong();
        range=(Range)Util.readStreamable(Range.class, in);
        sender=Util.readAddress(in);
    }

    @Override // GemStoneAddition
    public long size(short version) {
        // type (1 byte) + seqno (8 bytes)
        int retval=Global.BYTE_SIZE + Global.LONG_SIZE;

        retval+=Global.BYTE_SIZE; // presence for range
        if(range != null)
            retval+=2 * Global.LONG_SIZE; // 2 times 8 bytes for seqno
        retval+=Util.size(sender,version);
        return retval;
    }


    public NakAckHeader copy() {
        NakAckHeader ret=new NakAckHeader(type, seqno);
        ret.range=range;
        ret.sender=sender;
        return ret;
    }


    public static String type2Str(byte t) {
        switch(t) {
            case MSG:
                return "MSG";
            case XMIT_REQ:
                return "XMIT_REQ";
            case XMIT_RSP:
                return "XMIT_RSP";
            default:
                return "<undefined>";
        }
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("[").append(type2Str(type)).append(", seqno=").append(seqno);
        if(range != null)
            ret.append(", range=").append(range);
        if(sender != null) ret.append(", sender=" + sender);
        ret.append(']');
        return ret.toString();
    }


}
