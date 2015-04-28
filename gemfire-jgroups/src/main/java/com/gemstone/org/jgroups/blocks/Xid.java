/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Xid.java,v 1.5 2005/07/17 11:36:40 chrislott Exp $

package com.gemstone.org.jgroups.blocks;



import com.gemstone.org.jgroups.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;




/**
 * Distributed transaction ID modeled after the JTA spec. This is used to
 * identify entities of work, with which resources are associated. A transaction
 * will thus trigger the creation of an associated entry on each receiver, which
 * keeps track of resources acquired and their locks, operations to be committed
 * in case {@link ReplicationReceiver#commit(Xid)}is called.
 * <p>
 * A transaction is identified by its creator and a transaction ID. The creator
 * is currently a <a href="http://www.jgroups.com">JGroups</a> address,
 * consisting of the IP address and port of the member.
 * <p>
 * <em>Note that this class might be replaced in the future with the real
 * JTA counterpart.</em>
 * 
 * @author  <a href="mailto:belaban@yahoo.com">Bela Ban</a>.
 * @version $Revision: 1.5 $
 *
 * <p><b>Revisions:</b>
 *
 * <p>Dec 28 2002 Bela Ban: first implementation
 */
public class Xid implements Externalizable {
    protected Address     creator=null;
    protected long        id=0;
    protected int         mode=DIRTY_READS;
    static transient long next_id=0;
    public static final String XID="xid";

    /**
     * Writes are serialized, but reads can be dirty; e.g., a data might have
     * been changed while we read it. This is fast because we don't need to
     * acquire locks for reads.
     */
    public static final int DIRTY_READS     = 1;
    
    /**
     * Reads are dirty until another transaction actually commits; at that
     * points the modified data will be reflected here.
     */
    public static final int READ_COMMITTED  = 2;
    
    /**
     * Each read causes the data read to be copied to the private workspace, so
     * subsequent reads always read the private data.
     */
    public static final int REPEATABLE_READ = 3;
    
    /**
     * Reads and writes require locks. This is very costly, and is not
     * recommended (and currently not implemented either :-)).
     */
    public static final int SERIALIZABLE    = 4;


    public Xid() {
        ; // used by externalization
    }

    private Xid(Address creator, long id) {
        this.creator=creator; this.id=id;
    }

    private Xid(Address creator, long id, int mode) {
        this.creator=creator; this.id=id; this.mode=mode;
    }

    public Address getCreator() {return creator;}
    public long    getId()      {return id;}
    public long    getMode()    {return mode;}


    public static Xid create(Address creator) throws Exception {
        if(creator == null)
            throw new Exception("Xid.create(): creator == null");
        synchronized(Xid.class) {
            return new Xid(creator, ++next_id);
        }
    }

    public static Xid create(Address creator, int mode) throws Exception {
        if(creator == null)
            throw new Exception("Xid.create(): creator == null");
        synchronized(Xid.class) {
            return new Xid(creator, ++next_id, mode);
        }
    }

    public static String modeToString(int m) {
        switch(m) {
        case DIRTY_READS:     return "DIRTY_READS";
        case READ_COMMITTED:  return "READ_COMMITTED";
        case REPEATABLE_READ: return "REPEATABLE_READ";
        case SERIALIZABLE:    return "SERIALIZABLE";
        default:              return "<unknown>";
        }
    }

    @Override // GemStoneAddition
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Xid)) return false; // GemStoneAddition
        return compareTo(other) == 0;
    }

    @Override // GemStoneAddition
    public int hashCode() {
        return creator.hashCode() + (int)id;
    }

    public int compareTo(Object o) {
        Xid other;
        int comp;
        if(o == null || !(o instanceof Xid))
            throw new ClassCastException("Xid.compareTo(): comparison between different classes");
        other=(Xid)o;
        comp=creator.compareTo(other.getCreator());
        if(comp != 0) return comp;
        if(id < other.getId()) return -1;
        if(id > other.getId()) return 1;
        return 0;
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append('<').append(creator).append(">:").append(id);
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(creator);
        out.writeLong(id);
        out.writeInt(mode);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        creator=(Address)in.readObject();
        id=in.readLong();
        mode=in.readInt();
    }



}
