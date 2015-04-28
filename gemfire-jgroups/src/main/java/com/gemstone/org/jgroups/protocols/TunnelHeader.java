/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TunnelHeader.java,v 1.5 2004/09/15 16:21:11 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Header;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;




public class TunnelHeader extends Header {
    public String channel_name=null;

    public TunnelHeader() {} // used for externalization

    public TunnelHeader(String n) {channel_name=n;}

    @Override // GemStoneAddition
    public long size(short version) {
        return 100;
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[TUNNEL:channel_name=" + channel_name + ']';
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(channel_name);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        channel_name=(String)in.readObject();
    }


}
