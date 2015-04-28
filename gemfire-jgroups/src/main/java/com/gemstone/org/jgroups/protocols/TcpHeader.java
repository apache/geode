/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TcpHeader.java,v 1.4 2005/04/15 13:17:02 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.util.Streamable;

import java.io.*;




public class TcpHeader extends Header implements Streamable {
    public String group_addr=null;

    public TcpHeader() {
    } // used for externalization

    public TcpHeader(String n) {
        group_addr=n;
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[TCP:group_addr=" + group_addr + ']';
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(group_addr);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group_addr=(String)in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(group_addr);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        group_addr=in.readUTF();
    }

    @Override // GemStoneAddition
    public long size(short version) {
        return group_addr.length() +2;
    }
}
