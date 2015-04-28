/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PingRsp.java,v 1.9 2005/08/08 12:45:43 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;


public class PingRsp implements Serializable, Streamable {
    public Address own_addr=null;
    public Address coord_addr=null;
    public boolean is_server=false;
    private static final long serialVersionUID=3634334590904551586L;

    public PingRsp() {
        // externalization
    }

    public PingRsp(Address own_addr, Address coord_addr, boolean is_server) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
        this.is_server=is_server;
    }

    @Override // GemStoneAddition
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof PingRsp)) return false; // GemStoneAddition
        PingRsp other=(PingRsp)obj;
        return own_addr != null && other.own_addr != null && own_addr.equals(other.own_addr);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     * 
     * Note that we just need to make sure that equal objects return equal
     * hashcodes; nothing really elaborate is done here.
     */
    @Override // GemStoneAddition
    public int hashCode() { // GemStoneAddition
      int result = 0;
      if (this.own_addr != null) {
        result += own_addr.hashCode();
      }
      return result;
    }

    public boolean isCoord() {
        if(!is_server)
            return false;
        if(own_addr != null && coord_addr != null)
            return own_addr.equals(coord_addr);
        return false;
    }

    public int size(short version) {
        int retval=Global.BYTE_SIZE *3; // for is_server, plus 2 presence bytes
        retval+=2 * Global.BYTE_SIZE;   // 1 boolean for each address: IpAddress or other address ?
        if(own_addr != null)
            retval+=own_addr.size(version);
        if(coord_addr != null)
            retval+=coord_addr.size(version);
        return retval;
    }

    public Address getAddress() {
        return own_addr;
    }

    public Address getCoordAddress() {
        return coord_addr;
    }

    public boolean isServer() {
        return is_server;
    }

    @Override // GemStoneAddition
    public String toString() {
        return new StringBuffer("[own_addr=").append(own_addr).append(", coord_addr=").append(coord_addr).
                append(", is_server=").append(is_server).append(']').toString();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeAddress(own_addr, outstream);
        Util.writeAddress(coord_addr, outstream);
        outstream.writeBoolean(is_server);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        own_addr=Util.readAddress(instream);
        coord_addr=Util.readAddress(instream);
        is_server=instream.readBoolean();
    }
}
