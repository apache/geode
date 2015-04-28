/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: MergeData.java,v 1.4 2005/08/08 12:45:38 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.View;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Encapsulates data sent with a MERGE_RSP (handleMergeResponse()) and INSTALL_MERGE_VIEW
 * (handleMergeView()).
 *
 * @author Bela Ban Oct 22 2001
 */
public class MergeData implements Externalizable {
    Address   sender=null;
    boolean   merge_rejected=false;
    View      view=null;
    Digest    digest=null;

    /**
     * Empty constructor needed for externalization
     */
    public MergeData() {
    }

    public MergeData(Address sender, View view, Digest digest) {
        this.sender=sender;
        this.view=view;
        this.digest=digest;
    }

    public Address getSender() {
        return sender;
    }

    public View getView() {
        return view;
    }

    public Digest getDigest() {
        return digest;
    }

    public void setView(View v) {
        view=v;
    }

    public void setDigest(Digest d) {
        digest=d;
    }


    @Override // GemStoneAddition
    public boolean equals(Object other) {
        return sender != null && other != null && other instanceof MergeData &&
                ((MergeData)other).sender != null && ((MergeData)other).sender.equals(sender);
    }


    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     * 
     * Note that we just need to make sure that equal objects return equal
     * hashcodes; nothing really elaborate is done here.
     */
    @Override
    public int hashCode() { // GemStoneAddition
      return 0; // TODO more efficient implementation :-)
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sender);
        out.writeBoolean(merge_rejected);
        if(!merge_rejected) {
            out.writeObject(view);
            out.writeObject(digest);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sender=(Address)in.readObject();
        merge_rejected=in.readBoolean();
        if(!merge_rejected) {
            view=(View)in.readObject();
            digest=(Digest)in.readObject();
        }
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("sender=" + sender);
        if(merge_rejected)
            sb.append(" (merge_rejected)");
        else {
            sb.append(", view=" + view + ", digest=" + digest);
        }
        return sb.toString();
    }


}



