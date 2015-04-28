/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: UdpHeader.java,v 1.8 2005/04/20 09:10:09 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.util.Streamable;

import java.io.*;




public class UdpHeader extends Header implements Streamable {
    public String channel_name=null;
    int size=0;

    public UdpHeader() {
    }  // used for externalization

    public UdpHeader(String n) {
        channel_name=n;
        if(channel_name != null)
            size=channel_name.length()+2; // +2 for writeUTF()
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[UDP:channel_name=" + channel_name + ']';
    }


    @Override // GemStoneAddition
    public long size(short version) {
        return size;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(channel_name);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        channel_name=in.readUTF();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(channel_name);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        channel_name=in.readUTF();
        if(channel_name != null)
            size=channel_name.length()+2; // +2 for writeUTF()
    }
}
