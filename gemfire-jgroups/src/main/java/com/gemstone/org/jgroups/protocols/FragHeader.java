/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
//import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.util.Streamable;

import java.io.*;

/**
 * @author Bela Ban
 * @version $Id: FragHeader.java,v 1.2 2005/04/15 13:17:02 belaban Exp $
 */
public class FragHeader extends Header implements Streamable {
    public long id=0;
    public int  frag_id=0;
    public int  num_frags=0;
    /** GemStoneAddition - view id tracking */
    public long viewId;


    public FragHeader() {
    } // used for externalization

    public FragHeader(long id, int frag_id, int num_frags, long vid) {
        this.id=id;
        this.frag_id=frag_id;
        this.num_frags=num_frags;
        this.viewId = vid;
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + ']';
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
        // GemStoneAddition view tracking
        out.writeLong(this.viewId);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id=in.readLong();
        frag_id=in.readInt();
        num_frags=in.readInt();
        this.viewId = in.readLong();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(id);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
        // GemStoneAddition view tracking
        out.writeLong(this.viewId);
    }

    @Override // GemStoneAddition
    public long size(short version) {
        return Global.LONG_SIZE + 2*Global.INT_SIZE;
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=in.readLong();
        frag_id=in.readInt();
        num_frags=in.readInt();
        // GemStoneAddition view tracking
        this.viewId = in.readLong();
    }

}
