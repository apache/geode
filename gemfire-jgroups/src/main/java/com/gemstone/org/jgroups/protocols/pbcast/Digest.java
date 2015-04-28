/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Digest.java,v 1.17 2005/10/03 13:25:26 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;

import java.util.concurrent.ConcurrentHashMap;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;
import com.gemstone.org.jgroups.util.VersionedStreamable;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * A message digest, which is used by the PBCAST layer for gossiping (also used by NAKACK for
 * keeping track of current seqnos for all members). It contains pairs of senders and a range of seqnos
 * (low and high), where each sender is associated with its highest and lowest seqnos seen so far.  That
 * is, the lowest seqno which was not yet garbage-collected and the highest that was seen so far and is
 * deliverable (or was already delivered) to the application.  A range of [0 - 0] means no messages have
 * been received yet. 
 * <p> April 3 2001 (bela): Added high_seqnos_seen member. It is used to disseminate
 * information about the last (highest) message M received from a sender P. Since we might be using a
 * negative acknowledgment message numbering scheme, we would never know if the last message was
 * lost. Therefore we periodically gossip and include the last message seqno. Members who haven't seen
 * it (e.g. because msg was dropped) will request a retransmission. See DESIGN for details.
 * @author Bela Ban
 */
public class Digest implements Externalizable, VersionedStreamable {
    /** Map key is Address, value is Entry> */
    Map    senders=null;
    protected static final GemFireTracer log=GemFireTracer.getLog(Digest.class);
    static final boolean warn=log.isWarnEnabled();




    public Digest() {
    } // used for externalization

    public Digest(int size) {
        senders=createSenders(size);
    }


    @Override // GemStoneAddition
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        if (!(obj instanceof Digest)) return false; // GemStoneAddition
        Digest other=(Digest)obj;
        if(senders == null && other.senders == null)
            return true;
        if (senders == null) return false; // GemStoneAddition
        return senders.equals(other.senders);
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
      if (senders != null) {
        result += senders.hashCode();
      }
      return result;
    }



    public void add(Address sender, long low_seqno, long high_seqno) {
        add(sender, low_seqno, high_seqno, -1);
    }


    public void add(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        add(sender, new Entry(low_seqno, high_seqno, high_seqno_seen));
    }

    private void add(Address sender, Entry entry) {
        if(sender == null || entry == null) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.Digest_SENDER__0__OR_ENTRY__1_IS_NULL_WILL_NOT_ADD_ENTRY, new Object[] {sender, entry});
            return;
        }
        Object retval=senders.put(sender, entry);
        if(retval != null && warn)
            log.warn("entry for " + sender + " was overwritten with " + entry);
    }


    public void add(Digest d) {
        if(d != null) {
            Map.Entry entry;
            Address key;
            Entry val;
            for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(Address)entry.getKey();
                val=(Entry)entry.getValue();
                add(key, val.low_seqno, val.high_seqno, val.high_seqno_seen);
            }
        }
    }

    public void replace(Digest d) {
        if(d != null) {
            Map.Entry entry;
            Address key;
            Entry val;
            clear();
            for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(Address)entry.getKey();
                val=(Entry)entry.getValue();
                add(key, val.low_seqno, val.high_seqno, val.high_seqno_seen);
            }
        }
    }

    public Entry get(Address sender) {
        return (Entry)senders.get(sender);
    }

    public boolean set(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return false;
        entry.low_seqno=low_seqno;
        entry.high_seqno=high_seqno;
        entry.high_seqno_seen=high_seqno_seen;
        return true;
    }

    /**
     * Adds a digest to this digest. This digest must have enough space to add the other digest; otherwise an error
     * message will be written. For each sender in the other digest, the merge() method will be called.
     */
    public void merge(Digest d) {
        if(d == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Digest_DIGEST_TO_BE_MERGED_WITH_IS_NULL);
            return;
        }
        Map.Entry entry;
        Address sender;
        Entry val;
        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(val != null) {
                merge(sender, val.low_seqno, val.high_seqno, val.high_seqno_seen);
            }
        }
    }


    /**
     * Similar to add(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.low_seqno=min(this.low_seqno, low_seqno)
     * <li>this.high_seqno=max(this.high_seqno, high_seqno)
     * <li>this.high_seqno_seen=max(this.high_seqno_seen, high_seqno_seen)
     * </ol>
     * If the sender doesn not exist, a new entry will be added (provided there is enough space)
     */
    public void merge(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        if(sender == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Digest_SENDER__NULL);
            return;
        }
        Entry entry=(Entry)senders.get(sender);
        if(entry == null) {
            add(sender, low_seqno, high_seqno, high_seqno_seen);
        }
        else {
            if(low_seqno < entry.low_seqno)
                entry.low_seqno=low_seqno;
            if(high_seqno > entry.high_seqno)
                entry.high_seqno=high_seqno;
            if(high_seqno_seen > entry.high_seqno_seen)
                entry.high_seqno_seen=high_seqno_seen;
        }
    }



    public boolean contains(Address sender) {
        return senders.containsKey(sender);
    }


    /**
     * Compares two digests and returns true if the senders are the same, otherwise false.
     * @param other
     * @return True if senders are the same, otherwise false.
     */
    public boolean sameSenders(Digest other) {
        if(other == null) return false;
        if(this.senders == null || other.senders == null) return false;
        if(this.senders.size() != other.senders.size()) return false;

        Set my_senders=senders.keySet(), other_senders=other.senders.keySet();
        return my_senders.equals(other_senders);
    }


    /** 
     * Increments the sender's high_seqno by 1.
     */
    public void incrementHighSeqno(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return;
        entry.high_seqno++;
    }


    public int size() {
        return senders.size();
    }




    /**
     * Resets the seqnos for the sender at 'index' to 0. This happens when a member has left the group,
     * but it is still in the digest. Resetting its seqnos ensures that no-one will request a message
     * retransmission from the dead member.
     */
    public void resetAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.reset();
    }


    public void clear() {
        senders.clear();
    }

    public long lowSeqnoAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.low_seqno;
    }


    public long highSeqnoAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno;
    }


    public long highSeqnoSeenAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno_seen;
    }


    public void setHighSeqnoAt(Address sender, long high_seqno) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.high_seqno=high_seqno;
    }

    public void setHighSeqnoSeenAt(Address sender, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.high_seqno_seen=high_seqno_seen;
    }

    public void setHighestDeliveredAndSeenSeqnos(Address sender, long high_seqno, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null) {
            entry.high_seqno=high_seqno;
            entry.high_seqno_seen=high_seqno_seen;
        }
    }


    public Digest copy() {
        Digest ret=new Digest(senders.size());
        Map.Entry entry;
        Entry tmp;
        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            tmp=(Entry)entry.getValue();
            ret.add((Address)entry.getKey(), tmp.low_seqno, tmp.high_seqno, tmp.high_seqno_seen);
        }
        return ret;
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        if(senders == null) return "[]";
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                first=false;
            }
            sb.append(key).append(": ").append('[').append(val.low_seqno).append(" : ");
            sb.append(val.high_seqno);
            if(val.high_seqno_seen >= 0)
                sb.append(" (").append(val.high_seqno_seen).append(")");
            sb.append("]");
        }
        return sb.toString();
    }


    public String printHighSeqnos() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append('[');
                first=false;
            }
            sb.append(key).append("#").append(val.high_seqno);
        }
        sb.append(']');
        return sb.toString();
    }


    public String printHighSeqnosSeen() {
       StringBuffer sb=new StringBuffer();
        boolean first=true;
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append('[');
                first=false;
            }
            sb.append(key).append("#").append(val.high_seqno_seen);
        }
        sb.append(']');
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        toData(out);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fromData(in);
    }

    public void writeTo(DataOutputStream out) throws IOException {
      toData(out);
    }
    
    public void toData(DataOutput out) throws IOException {
        out.writeShort(senders.size());
        Map.Entry entry;
        Address key;
        Entry val;
        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            JChannel.getGfFunctions().writeObject(key, out);
            out.writeLong(val.low_seqno);
            out.writeLong(val.high_seqno);
            out.writeLong(val.high_seqno_seen);
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      try {
        fromData(in);
      } catch (ClassNotFoundException e) {
        InstantiationException ex = new InstantiationException("Unable to instantiate during deserialization");
        ex.initCause(e);
        throw ex;
      }
    }
    
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
        short size=in.readShort();
        senders=createSenders(size);
        Address key;
        for(int i=0; i < size; i++) {
            key=JChannel.getGfFunctions().readObject(in);
            long low_seqno = in.readLong();
            long high_seqno = in.readLong();
            long high_seqno_seen = in.readLong();
            add(key, low_seqno, high_seqno, high_seqno_seen);
        }
    }


    public long serializedSize(short version) {
        long retval=Global.SHORT_SIZE; // number of elements in 'senders'
        if(senders.size() > 0) {
            Address addr=(Address)senders.keySet().iterator().next();
            int len=addr.size(version) +
                    2 * Global.BYTE_SIZE; // presence byte, IpAddress vs other address
            len+=3 * Global.LONG_SIZE; // 3 longs in one Entry
            retval+=len * senders.size();
        }
        return retval;
    }

    private Map createSenders(int size) {
        return new ConcurrentHashMap(size);
    }


    /**
     * Class keeping track of the lowest and highest sequence numbers delivered, and the highest
     * sequence numbers received, per member
     */
    public static class Entry  {
        public long low_seqno, high_seqno, high_seqno_seen=-1;

        public Entry(long low_seqno, long high_seqno, long high_seqno_seen) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
            this.high_seqno_seen=high_seqno_seen;
        }

        public Entry(long low_seqno, long high_seqno) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
        }

        public Entry(Entry other) {
            if(other != null) {
                low_seqno=other.low_seqno;
                high_seqno=other.high_seqno;
                high_seqno_seen=other.high_seqno_seen;
            }
        }

        @Override // GemStoneAddition
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Entry)) return false; // GemStoneAddition
            Entry other=(Entry)obj;
            return low_seqno == other.low_seqno && high_seqno == other.high_seqno && high_seqno_seen == other.high_seqno_seen;
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
          result += this.low_seqno;
          result += this.high_seqno;
          result += this.high_seqno_seen;
          return result;
        }

        @Override // GemStoneAddition
        public String toString() {
            return new StringBuffer("low=").append(low_seqno).append(", high=").append(high_seqno).
                    append(", highest seen=").append(high_seqno_seen).toString();
        }

        public void reset() {
            low_seqno=high_seqno=0;
            high_seqno_seen=-1;
        }
    }


    @Override
    public short[] getSerializationVersions() {
      return null;
    }
}
