/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: MergeView.java,v 1.5 2005/11/21 13:33:08 belaban Exp $


package com.gemstone.org.jgroups;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Vector;


/**
 * A view that is sent as a result of a merge.
 * Whenever a group splits into subgroups, e.g., due to a network partition, 
 * and later the subgroups merge back together, a MergeView instead of a View 
 * will be received by the application. The MergeView class is a subclass of 
 * View and contains as additional instance variable: the list of views that 
 * were merged. For example, if the group denoted by view V1:(p,q,r,s,t) 
 * splits into subgroups V2:(p,q,r) and V2:(s,t), the merged view might be 
 * V3:(p,q,r,s,t). In this case the MergeView would contain a list of 2 views: 
 * V2:(p,q,r) and V2:(s,t).
 */
public class MergeView extends View {
    protected Vector subgroups=null; // subgroups that merged into this single view (a list of Views)


    /**
     * Used by externalization
     */
    public MergeView() {
    }


    /**
     * Creates a new view
     *
     * @param vid       The view id of this view (can not be null)
     * @param members   Contains a list of all the members in the view, can be empty but not null.
     * @param subgroups A list of Views representing the former subgroups
     */
    public MergeView(ViewId vid, Vector members, Vector subgroups) {
        super(vid, members);
        this.subgroups=subgroups;
    }


    /**
     * Creates a new view
     *
     * @param creator   The creator of this view (can not be null)
     * @param id        The lamport timestamp of this view
     * @param members   Contains a list of all the members in the view, can be empty but not null.
     * @param subgroups A list of Views representing the former subgroups
     */
    public MergeView(Address creator, long id, Vector members, Vector subgroups) {
        super(creator, id, members);
        this.subgroups=subgroups;
    }

    @Override // GemStoneAddition
    public boolean equals(Object o) { // GemStoneAddition for findbugs
      return super.equals(o);
    }
    
    @Override // GemStoneAddition
    public int hashCode() { // GemStoneAddition for findbugs
      return super.hashCode();
    }

    public Vector getSubgroups() {
        return subgroups;
    }


    /**
     * creates a copy of this view
     *
     * @return a copy of this view
     */
    @Override // GemStoneAddition
    public Object clone() {
        ViewId vid2=vid != null ? (ViewId)vid.clone() : null;
        Vector members2=members != null ? (Vector)members.clone() : null;
        Vector subgroups2=subgroups != null ? (Vector)subgroups.clone() : null;
        return new MergeView(vid2, members2, subgroups2);
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("MergeView::" + super.toString());
        sb.append(", subgroups=" + subgroups);
        return sb.toString();
    }


    @Override // GemStoneAddition
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(subgroups);
    }


    @Override // GemStoneAddition
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        subgroups=(Vector)in.readObject();
    }


    @Override // GemStoneAddition
    public void writeTo(DataOutputStream out) throws IOException {
        super.writeTo(out);

        // write subgroups
        int len=subgroups != null? subgroups.size() : 0;
        out.writeShort(len);
        if(len == 0)
            return;
        View v;
        for(Iterator it=subgroups.iterator(); it.hasNext();) {
            v=(View)it.next();
            v.writeTo(out);
        }
    }

    @Override // GemStoneAddition
    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        super.readFrom(in);
        short len=in.readShort();
        if(len > 0) {
            View v;
            subgroups=new Vector();
            for(int i=0; i < len; i++) {
                v=new View();
                v.readFrom(in);
                subgroups.add(v);
            }
        }
    }

    @Override // GemStoneAddition
    public int serializedSize(short version) {
        int retval=super.serializedSize(version);
        retval+=Global.SHORT_SIZE; // for size of subgroups vector

        if(subgroups == null)
            return retval;
        View v;
        for(Iterator it=subgroups.iterator(); it.hasNext();) {
            v=(View)it.next();
            retval+=v.serializedSize(version);
        }
        return retval;
    }


}
