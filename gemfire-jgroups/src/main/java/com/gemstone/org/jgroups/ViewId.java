/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ViewId.java,v 1.10 2005/07/12 11:45:42 belaban Exp $

package com.gemstone.org.jgroups;

import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;
import com.gemstone.org.jgroups.util.VersionedStreamable;

import java.io.*;


/**
 * ViewIds are used for ordering views (each view has a ViewId and a list of members).
 * Ordering between views is important for example in a virtual synchrony protocol where
 * all views seen by a member have to be ordered.
 */
public class ViewId implements Externalizable, Comparable, Cloneable, VersionedStreamable {
    Address coord_addr=null;   // Address of the issuer of this view
    long id=0;                 // Lamport time of the view


    public ViewId() { // used for externalization
    }


    /**
     * Creates a ViewID with the coordinator address and a Lamport timestamp of 0.
     *
     * @param coord_addr the address of the member that issued this view
     */
    public ViewId(Address coord_addr) {
        this.coord_addr=coord_addr;
    }

    /**
     * Creates a ViewID with the coordinator address and the given Lamport timestamp.
     *
     * @param coord_addr - the address of the member that issued this view
     * @param id         - the Lamport timestamp of the view
     */
    public ViewId(Address coord_addr, long id) {
        this.coord_addr=coord_addr;
        this.id=id;
    }

    /**
     * returns the lamport time of the view
     *
     * @return the lamport time timestamp
     */
    public long getId() {
        return id;
    }


    /**
     * returns the address of the member that issued this view
     *
     * @return the Address of the the issuer
     */
    public Address getCoordAddress() {
        return coord_addr;
    }


    @Override // GemStoneAddition
    public String toString() {
        return "[" + coord_addr + '|' + id + ']';
    }

    /**
     * Cloneable interface
     * Returns a new ViewID object containing the same address and lamport timestamp as this view
     */
    @Override // GemStoneAddition
    public Object clone() {
        return new ViewId(coord_addr, id);
    }

    /**
     * Old Copy method, deprecated because it is substituted by clone()
     */
    public ViewId copy() {
        return (ViewId)clone();
    }

    /**
     * Establishes an order between 2 ViewIds. First compare on id. <em>Compare on coord_addr
     * only if necessary</em> (i.e. ids are equal) !
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareTo(Object other) {
        if(other == null) return 1; //+++ Maybe necessary to throw an exception

        if(!(other instanceof ViewId)) {
            throw new ClassCastException("ViewId.compareTo(): view id is not comparable with different Objects");
        }
        return id > ((ViewId)other).id ? 1 : id < ((ViewId)other).id ? -1 : 0;
    }

    /**
     * Old Compare
     */
    public int compare(Object o) {
        return compareTo(o);
    }


    @Override // GemStoneAddition
    public boolean equals(Object other_view) {
      if (other_view == null || !(other_view instanceof ViewId)) return false; // GemStoneAddition
        return compareTo(other_view) == 0 ? true : false;
    }


    @Override // GemStoneAddition
    public int hashCode() {
        return (int)id;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(coord_addr);
        out.writeLong(id);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        coord_addr=(Address)in.readObject();
        id=in.readLong();
    }

    public void writeTo(DataOutputStream out) throws IOException {
      JChannel.getGfFunctions().invokeToData(this, out);
    }
    
    public void toData(DataOutput out) throws IOException {
        JChannel.getGfFunctions().writeObject(coord_addr, out);
        out.writeLong(id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      try {
        JChannel.getGfFunctions().invokeFromData(this, in);
      } catch (Exception e) {
        InstantiationException ex = new InstantiationException("problem deserializing ViewId");
        ex.initCause(e);
        throw ex;
      }
    }
    
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
        coord_addr=JChannel.getGfFunctions().readObject(in);
        id=in.readLong();
    }

    public int serializedSize(short version) {
        int retval=Global.LONG_SIZE; // for the id
        retval+=Util.size(coord_addr,version);
        return retval;
    }


    @Override
    public short[] getSerializationVersions() {
      return null;
    }

}
