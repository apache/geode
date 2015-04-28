/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PingHeader.java,v 1.10 2005/04/26 15:22:13 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;


public class PingHeader extends Header implements Streamable {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingRsp(local_addr, coord_addr)

    public byte type=0;
    public PingRsp arg=null;

    public PingHeader() {
    } // for externalization

    public PingHeader(byte type, PingRsp arg) {
        this.type=type;
        this.arg=arg;
    }

    @Override // GemStoneAddition
    public long size(short version) {
        long retval=Global.BYTE_SIZE *2; // type and presence
        if(arg != null) {
            retval+=arg.size(version);
        }
        return retval;
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[PING: type=" + type2Str(type) + ", arg=" + arg + ']';
    }

    String type2Str(byte t) {
        switch(t) {
            case GET_MBRS_REQ:
                return "GET_MBRS_REQ";
            case GET_MBRS_RSP:
                return "GET_MBRS_RSP";
            default:
                return "<unknown type (" + t + ")>";
        }
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeObject(arg);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        arg=(PingRsp)in.readObject();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        outstream.writeByte(type);
        Util.writeStreamable(arg, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        type=instream.readByte();
        arg=(PingRsp)Util.readStreamable(PingRsp.class, instream);
    }
}
