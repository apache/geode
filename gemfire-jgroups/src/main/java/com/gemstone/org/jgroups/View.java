/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: View.java,v 1.10 2005/08/08 09:48:06 belaban Exp $

package com.gemstone.org.jgroups;


import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import com.gemstone.org.jgroups.protocols.pbcast.Digest;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.StreamableFixedID;
import com.gemstone.org.jgroups.util.Util;
import com.gemstone.org.jgroups.util.VersionedStreamable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A view is a local representation of the current membership of a group.
 * Only one view is installed in a channel at a time.
 * Views contain the address of its creator, an ID and a list of member addresses.
 * These adresses are ordered, and the first address is always the coordinator of the view.
 * This way, each member of the group knows who the new coordinator will be if the current one
 * crashes or leaves the group.
 * The views are sent between members using the VIEW_CHANGE event.
 */
public class View implements Externalizable, Cloneable, StreamableFixedID {
  
  /** GemStoneAddition - added for size check on views that carry;
   * credentials
   */
//  public static int MAX_VIEW_SIZE = 60000;
  
    /* A view is uniquely identified by its ViewID
     * The view id contains the creator address and a Lamport time.
     * The Lamport time is the highest timestamp seen or sent from a view.
     * if a view change comes in with a lower Lamport time, the event is discarded.
     */
    protected ViewId vid;

    /**
     * A list containing all the members of the view
     * This list is always ordered, with the coordinator being the first member.
     * the second member will be the new coordinator if the current one disappears
     * or leaves the group.
     */
    protected Vector members;

    /**
     * GemStoneAddition -- any additional payload to be sent
     * Used by the security AUTH layer to add/verify credentials.
     */
    private Object additionalData;
    
    /**
     * GemStoneAddition - size of serialized form of additionalData
     */
    private int additionalDataSize;

    /**
     * GemStoneAddition - Members removed from previous view as Suspects
     */
    private Set suspectedMembers;
    
    /**
     * GemStoneAddition - message digest moved from GmsHeader for FRAG3
     * fragmentation
     */
    private Digest messageDigest;
    
    /**
     * creates an empty view, should not be used
     */
    public View() {
    }


    /**
     * Creates a new view
     *
     * @param vid     The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId vid, Vector members) {
        this.vid=vid;
        this.members=members;
    }


    /**
     * Creates a new view
     *
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(Address creator, long id, Vector members) {
      this(new ViewId(creator, id), members);
    }

    /**
     * Creates a new view
     *
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     * @param suspectedMembers GemStoneAddition - tracking of ousted members
     */
    public View(Address creator, long id, Vector members, Vector suspectedMembers) {
        this(new ViewId(creator, id), members);
        if (suspectedMembers != null) {
          this.suspectedMembers = new HashSet(suspectedMembers);
        }
    }


    /**
     * returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the view ID of this view
     */
    public ViewId getVid() {
        return vid;
    }

    /**
     * returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator() {
        return vid != null ? vid.getCoordAddress() : null;
    }
    
    /**
     * returns the coordinator of this view, which may not be the creator if it is not in the view.
     *  GemStoneAddition
     */
    public Address getCoordinator() {

      if (this.members.size() < 1) {
        return null;
      }
      
      return new Membership(this.members).getCoordinator();
    }

    /**
     * Returns a reference to the List of members (ordered)
     * Do NOT change this list, hence your will invalidate the view
     * Make a copy if you have to modify it.
     *
     * @return a reference to the ordered list of members in this view
     */
    public Vector getMembers() {
        return members;
    }
    
    /**
     * GemStoneAddition
     * get the members that were removed from the previous view due to
     * Suspect processing
     */
    public Set getSuspectedMembers() {
      return this.suspectedMembers == null? Collections.EMPTY_SET : this.suspectedMembers;
    }

    /**
     * GemStoneAddition -- getter for additional data
     */
    public Object getAdditionalData() {
      return this.additionalData;
    }

    /**
     * GemStoneAddition -- setter for additional data
     */
    public void setAdditionalData(Object data) {
      this.additionalData = data;
      // until credential fragmentation is implemented, we must perform
      // a size check to make sure the view fits into a datagram
      try {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(10000);
        ObjectOutputStream oos = new ObjectOutputStream(bas);
        oos.writeObject(data);
        this.additionalDataSize = bas.size();
//        if (serializedSize() > MAX_VIEW_SIZE) {
//          this.additionalData = null;
//          this.additionalDataSize = 0;
//          throw new IllegalArgumentException(
//            JGroupsStrings.View_SERIALIZED_VIEW_SIZE_0_EXCEEDS_MAXIMUM_OF_1
//              .toLocalizedString(new Object[] { Integer.valueOf(bas.size()), Integer.valueOf(MAX_VIEW_SIZE) }));
//        }
      }
      catch (IOException e) {
        // ignore - this will happen again when the view is serialized
        // for transmission
      }
    }

    /**
     * returns true, if this view contains a certain member
     *
     * @param mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     *         if the argument mbr is null, this operation returns false
     */
    public boolean containsMember(Address mbr) {
        if(mbr == null || members == null) {
            return false;
        }
        return members.contains(mbr);
    }
    
    /**
     * GemStoneAddition - removes the given address from the set of suspected members
     * @param mbr
     */
    public void notSuspect(Address mbr) {
      this.suspectedMembers.remove(mbr);
    }
    
    /**
     * GemStoneAddition - retrieve the address in the view that corresponds
     * to the given address.  If the address is not in the view, return
     * the argument.
     */
    public Address getMember(Address addr) {
      int sz = members.size();
      // reverse search to find the newest member that matches, in case
      // of membership race condition that includes an old ID and a new ID
      // for a member that's joining
      for (int i=sz-1; i>0; i--) {
        Address mbr = (Address)members.get(i);
        if (mbr.equals(addr)) {
          return mbr;
        }
      }
      return addr;
    }


    @Override // GemStoneAddition
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        if(vid != null) {
          if (!(obj instanceof View)) return false; // GemStoneAddition
            int rc=vid.compareTo(((View)obj).vid);
            if(rc != 0)
                return false;
            if(members != null && ((View)obj).members != null) {
                return members.equals(((View)obj).members);
            }
        }
        return false;
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
      if (vid != null) {
        result += vid.hashCode();
        if (members != null) {
          result += members.hashCode();
        }
      }
      return result;
    }
    
    /**
     * returns the number of members in this view
     *
     * @return the number of members in this view 0..n
     */
    public int size() {
        return members == null ? 0 : members.size();
    }


    /**
     * creates a copy of this view
     *
     * @return a copy of this view
     */
    @Override // GemStoneAddition
    @SuppressFBWarnings(value="CN_IDIOM_NO_SUPER_CALL")
    public Object clone() {
        ViewId vid2=vid != null ? (ViewId)vid.clone() : null;
        Vector members2=members != null ? (Vector)members.clone() : null;
        View result = new View(vid2, members2);
        if (this.suspectedMembers != null) {
          result.suspectedMembers = new HashSet(this.suspectedMembers);
        }
        if (additionalData != null) {
          result.additionalData = additionalData;
          result.additionalDataSize = additionalDataSize;
        }
        return result;
    }


    /**
     * debug only
     */
    public String printDetails() {
        StringBuffer ret=new StringBuffer();
        ret.append(vid).append("\n\t");
        if(members != null) {
            for(int i=0; i < members.size(); i++) {
                ret.append(members.elementAt(i)).append("\n\t");
            }
            ret.append('\n');
        }
        return ret.toString();
    }


    // GemStoneAddition - get the member name for an address
    private String memberName(Address m) {
      if (!(m instanceof IpAddress))
        return m.toString();
      IpAddress im = (IpAddress)m;
      StringBuffer sb = new StringBuffer();

      sb.append(im.toString());
      int port = im.getDirectPort(); 
      if (port > 0) {
        sb.append('/');
        sb.append(port);
      }
      return sb.toString();
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer(64);
        // GemStoneAddition - give more info than jgroups defaults to giving
        ret.append(vid);
        ret.append(" [");
        for (Iterator iter = members.iterator(); iter.hasNext(); ) {
          Address member = (Address) iter.next();
          // GemStoneAddition
          ret.append(memberName(member));
          
          if (iter.hasNext()) {
            ret.append(", ");
          }
        }
        // GemStoneAddition
        if (this.additionalData != null) {
          ret.append(this.additionalData);
        }
        ret.append("]");
        if (this.suspectedMembers != null && this.suspectedMembers.size() > 0) {
          ret.append(" crashed mbrs: [");
          for (Iterator it=this.suspectedMembers.iterator(); it.hasNext(); ) {
            ret.append(memberName((Address)it.next()));
            if (it.hasNext()) {
              ret.append(", ");
            }
          }
          ret.append(']');
        }
        //ret.append(vid).append(" ").append(members);
        return ret.toString();
    }
    //public String toString() {
    //    StringBuffer ret=new StringBuffer(64);
    //    ret.append(vid).append(" ").append(members);
    //    return ret.toString();
    //}
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vid);
        out.writeObject(members);
        out.writeObject(this.suspectedMembers); // GemStoneAddition
        out.writeInt(this.additionalDataSize);
        out.writeObject(this.additionalData); // GemStoneAddition
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vid=(ViewId)in.readObject();
        members=(Vector)in.readObject();
        this.suspectedMembers = (Set)in.readObject(); // GemStoneAddition
        this.additionalDataSize = in.readInt();
        this.additionalData = in.readObject(); // GemStoneAddition
    }
    
    /** GemStoneAddition - find the lead member in this view */
    public Address getLeadMember() {
      for (int i=0; i<members.size(); i++) {
        Address mbr = (Address)members.get(i);
        if (((IpAddress)mbr).getVmKind() == 10) {
          return mbr;
        }
      }
      return null;
    }


    public void writeTo(DataOutputStream out) throws IOException {
      JChannel.getGfFunctions().invokeToData(this, out);
    }
    
    public void toData(DataOutput out) throws IOException {
        // vid
        if(vid != null) {
            out.writeBoolean(true);
            JChannel.getGfFunctions().invokeToData(vid, out);
        }
        else
            out.writeBoolean(false);

        // members:
        JChannel.getGfFunctions().writeObject(members, out);
        // GemStoneAddition - suspectedMembers
        if (this.suspectedMembers == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          JChannel.getGfFunctions().writeObject(this.suspectedMembers, out);
        }
        JChannel.getGfFunctions().writeObject(this.messageDigest, out);
        // GemStoneAddition
        out.writeInt(this.additionalDataSize);
        JChannel.getGfFunctions().writeObject(this.additionalData, out);
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      try {
        JChannel.getGfFunctions().invokeFromData(this, in);
      } catch (ClassNotFoundException ex) {
        throw new IllegalAccessException(
            ExternalStrings.View_COULD_NOT_READ_ADDITIONAL_DATA_0
              .toLocalizedString(ex));
      }

    }
    
    public int getDSFID() {
      return JGROUPS_VIEW;
    }
    
    public short[] getSerializationVersions() {
      return null;
    }
    
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
        boolean b;
        // vid:
        b=in.readBoolean();
        if(b) {
            vid=new ViewId();
            JChannel.getGfFunctions().invokeFromData(vid, in);
        }

        // members:
        members=JChannel.getGfFunctions().readObject(in);
        
        // GemStoneAddition - suspectedMembers
        if (in.readBoolean()) {
          this.suspectedMembers = JChannel.getGfFunctions().readObject(in);
        }
        this.messageDigest = JChannel.getGfFunctions().readObject(in);
        // GemStoneAddition
        this.additionalDataSize = in.readInt();
        this.additionalData = JChannel.getGfFunctions().readObject(in);
    }

    public int serializedSize(short version) {
        int retval=Global.BYTE_SIZE; // presence for vid
        if(vid != null)
            retval+=vid.serializedSize(version);
        retval+=Util.size(members,version);
        // GemStoneAddition - suspectedMembers
        if (this.suspectedMembers != null) {
          retval+=Util.size(this.suspectedMembers, version);
        }
        if (this.additionalData != null) {
          retval += this.additionalDataSize;
        }
        return retval;
    }


    public void setMessageDigest(Digest messageDigest) {
      this.messageDigest = messageDigest;
    }


    public Digest getMessageDigest() {
      return messageDigest;
    }


}
