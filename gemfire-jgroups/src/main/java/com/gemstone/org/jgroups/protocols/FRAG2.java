/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FRAG2.java,v 1.20 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Range;
import com.gemstone.org.jgroups.util.Util;


/**
 * Fragmentation layer. Fragments messages larger than frag_size into smaller packets.
 * Reassembles fragmented packets into bigger ones. The fragmentation number is prepended
 * to the messages as a header (and removed at the receiving side).<p>
 * Each fragment is identified by (a) the sender (part of the message to which the header is appended),
 * (b) the fragmentation ID (which is unique per FRAG2 layer (monotonically increasing) and (c) the
 * fragement ID which ranges from 0 to number_of_fragments-1.<p>
 * Requirement: lossless delivery (e.g. NAK, ACK). No requirement on ordering. Works for both unicast and
 * multicast messages.<br/>
 * Compared to FRAG, this protocol does <em>not</em> need to serialize the message in order to break it into
 * smaller fragments: it looks only at the message's buffer, which is a byte[] array anyway. We assume that the
 * size addition for headers and src and dest address is minimal when the transport finally has to serialize the
 * message, so we add a constant (1000 bytes).
 * @author Bela Ban
 * @version $Id: FRAG2.java,v 1.20 2005/08/11 12:43:47 belaban Exp $
 */
public class FRAG2 extends Protocol  {
  
    public static boolean DEBUG_FRAG2 = Boolean.getBoolean("gemfire.debug-frag2");

    /** The max number of bytes in a message. If a message's buffer is bigger, it will be fragmented */
    int frag_size=1500;

    /** Number of bytes that we think the headers plus src and dest will take up when
        message is serialized by transport. This will be subtracted from frag_size */
    int overhead=50;

    /*the fragmentation list contains a fragmentation table per sender
     *this way it becomes easier to clean up if a sender (member) leaves or crashes
     */
    private final FragmentationList    fragment_list=new FragmentationList();
    private int                        curr_id=1;
    private final Vector               members=new Vector(11);
    static String  name="FRAG2"; // GemStone - increased visibility for DirAck

    long num_sent_msgs=0;
    long num_sent_frags=0;
    long num_received_msgs=0;
    long num_received_frags=0;
    
//    DirAck dirAckProtocol;
    
    /** GemStoneAddition - birth view id */
    private long initialViewId;
    
    /** GemStoneAddition - current view id */
    private long currentViewId;

    /** GemStoneAddition - messages that should be queued before getting a view id */
    private List preJoinMessages = new LinkedList();

    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }
    
    // start GemStoneAddition
    @Override // GemStoneAddition  
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumFRAG2;
    }
    // end GemStone addition



    public int getFragSize() {return frag_size;}
    public void setFragSize(int s) {frag_size=s;}
    public int getOverhead() {return overhead;}
    public void setOverhead(int o) {overhead=o;}
    public long getNumberOfSentMessages() {return num_sent_msgs;}
    public long getNumberOfSentFragments() {return num_sent_frags;}
    public long getNumberOfReceivedMessages() {return num_received_msgs;}
    public long getNumberOfReceivedFragments() {return num_received_frags;}


    synchronized int getNextId() {
        return curr_id++;
    }

    /** Setup the Protocol instance acording to the configuration string */
    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("frag_size");
        if(str != null) {
            frag_size=Integer.parseInt(str);
            props.remove("frag_size");
        }

        str=props.getProperty("overhead");
        if(str != null) {
            overhead=Integer.parseInt(str);
            props.remove("overhead");
        }

        int old_frag_size=frag_size;
        frag_size-=overhead;
        if(frag_size <=0) {
            log.error("frag_size=" + old_frag_size + ", overhead=" + overhead +
                      ", new frag_size=" + frag_size + ": new frag_size is invalid");
            return false;
        }

        if(log.isInfoEnabled())
            log.info(ExternalStrings.FRAG2_FRAG_SIZE_0__OVERHEAD_1__NEW_FRAG_SIZE_2, 
              new Object[] {old_frag_size, overhead, frag_size});

        if(props.size() > 0) {
            log.error(ExternalStrings.FRAG2_FRAG2SETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        return true;
    }
    
    @Override // GemStoneAddition  
    public void start() throws Exception {
//      dirAckProtocol = (DirAck)stack.findProtocol(DirAck.name); // GemStoneAddition
    }


    @Override // GemStoneAddition  
    public void resetStats() {
        super.resetStats();
        num_sent_msgs=num_sent_frags=num_received_msgs=num_received_frags=0;
    }



    /**
     * Fragment a packet if larger than frag_size (add a header). Otherwise just pass down. Only
     * add a header if framentation is needed !
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            long size=msg.getLength();
            synchronized(this) {
                num_sent_msgs++;
            }
            if(size > frag_size) {
                if(trace) {
                    StringBuffer sb=new StringBuffer("message's buffer size is ");
                    sb.append(size).append(", will fragment ").append("(frag_size=");
                    sb.append(frag_size).append(')');
                    log.trace(sb.toString());
                }
                gf_fragment(msg); // GemStoneAddition
                //fragment(msg);  // Fragment and pass down
                recordSize();
               return;
            }
            break;

        case Event.VIEW_CHANGE:
          long initId = this.initialViewId;
          viewChanged(evt);
            synchronized(this.preJoinMessages) { // GemStoneAddition - preJoinMessages
              if (initId == 0 && this.initialViewId != 0) {
                for (Iterator it=this.preJoinMessages.iterator(); it.hasNext(); /**/) {
                  Message m = (Message)it.next();
                  unfragment(m);
                  recordSize();
                }
                this.preJoinMessages.clear();
              }
            }
            break;

        case Event.CONFIG:
            passDown(evt);
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            return;
        }

        passDown(evt);  // Pass on to the layer below us
    }


    /**
     * If event is a message, if it is fragmented, re-assemble fragments into big message and pass up
     * the stack.
     */
    @Override // GemStoneAddition  
    public void up(Event evt) {
        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Object obj=msg.getHeader(getName());
            if(obj != null && obj instanceof FragHeader) { // needs to be defragmented
                unfragment(msg); // Unfragment and possibly pass up
                recordSize();
                return;
            }
            else {
                num_received_msgs++;
            }
            break;

        case Event.CONFIG:
            passUp(evt);
            if(log.isInfoEnabled()) log.info(ExternalStrings.FRAG2_RECEIVED_CONFIG_EVENT__0, (Object)evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            return;
            
            // GemStoneAddition - track the birth and current view ids
        case Event.VIEW_CHANGE:
          // members anymore !
          long initId = this.initialViewId;
          viewChanged(evt);
          synchronized(this.preJoinMessages) {
            if (initId == 0 && this.initialViewId != 0) {
              for (Iterator it=this.preJoinMessages.iterator(); it.hasNext(); /**/) {
                Message m = (Message)it.next();
                unfragment(m);
                recordSize();
              }
              this.preJoinMessages.clear();
            }
          }
          break;
        }

        passUp(evt); // Pass up to the layer above us by default
    }
    
    private void viewChanged(Event evt) {
      // GemStoneAddition - track the initial view id and the current
      // view id.  Some fragments of early messages may be thrown away
      // or queued by DirAck, and FRAG2 may hold corresponding fragments,
      // so it needs to do the same thing.
      ViewId vid = ((View)evt.getArg()).getVid();
      if (this.initialViewId == 0) {
        this.initialViewId = vid.getId();
      }
      this.currentViewId = vid.getId();
      
      
      /* gemstoneaddition - moved this code out of down() so it could be used
       * in up(), where view changes are actually seen
       */
      
      //don't do anything if this dude is sending out the view change
      //we are receiving a view change,
      //in here we check for the
      View view=(View)evt.getArg();
      Vector new_mbrs=view.getMembers(), left_mbrs;
      Address mbr;

      left_mbrs=Util.determineLeftMembers(members, new_mbrs);
      synchronized(members) { // GemStoneAddition - synchronize all access to members collection
        members.clear();
        members.addAll(new_mbrs);
      }

      for(int i=0; i < left_mbrs.size(); i++) {
          mbr=(Address)left_mbrs.elementAt(i);
          //the new view doesn't contain the sender, he must have left,
          //hence we will clear all his fragmentation tables
          fragment_list.remove(mbr);
          if(trace) log.trace("removed " + mbr + " from fragmentation table");
      }
      recordSize();
    }
    
    
    private void recordSize() { //  GemstoneAddition - memory debugging
//      if (DEBUG_FRAG2) {
//        stack.gemfireStats.setJg3(this.fragment_list.size());
//      }
    }
    
    /**
     * GemStoneAddition - [obsolete] check for dirAck multiple destinations.  If found,
     * copy the message for each dest.  An assertion in DirAck will check to
     * make sure this has happened
     */
    void gf_fragment(Message msg) {
      fragment(msg);
    }


    /** Send all fragments as separate messages (with same ID !).
     Example:
     <pre>
     Given the generated ID is 2344, number of fragments=3, message {dst,src,buf}
     would be fragmented into:

     [2344,3,0]{dst,src,buf1},
     [2344,3,1]{dst,src,buf2} and
     [2344,3,2]{dst,src,buf3}
     </pre>
     */
    void fragment(Message msg) {
//      long start; // GemStoneAddition
        byte[]             buffer;
        List               fragments;
        Event              evt;
        FragHeader         hdr;
        Message            frag_msg;
        Address            dest=msg.getDest();
        long               id=0; // GemStoneAddition getNextId(); // used as seqnos
        int                num_frags;
        StringBuffer       sb;
        Range              r;

        try {
            hdr = (FragHeader)msg.getHeader(getName());
            id = getNextId();
            buffer=msg.getBuffer();
            fragments=Util.computeFragOffsets(buffer, frag_size);
            num_frags=fragments.size();
            if (stack != null) {
              stack.gfPeerFunctions.incJgFragmentsCreated(num_frags);
              stack.gfPeerFunctions.incJgFragmentationsPerformed();
            }
            //synchronized(this) {
            //    num_sent_frags+=num_frags;
            //}

            if(trace) {
                sb=new StringBuffer("fragmenting packet to ");
                sb.append((dest != null ? dest.toString() : "<all members>")).append(" (size=").append(buffer.length);
                sb.append(") into ").append(num_frags).append(" fragment(s) [frag_size=").append(frag_size).append(']');
                log.trace(sb.toString());
            }
            
            int fsize = fragments.size(); // GemStoneAddition

            for(int i=0; i < fsize; i++) {
                r=(Range)fragments.get(i);
                // Copy the original msg (needed because we need to copy the headers too)
                frag_msg=msg.copy(false); // don't copy the buffer, only src, dest and headers
                frag_msg.bundleable = false; // GemStoneAddition
                frag_msg.setBuffer(buffer, (int)r.low, (int)r.high);
                // GemStoneAddition:
                // send the view id with the message
                hdr=new FragHeader(id, i, num_frags, this.currentViewId);
                frag_msg.putHeader(getName(), hdr);
                // GemStoneAddition - only the last fragment is processed in DirAck
                evt=new Event(Event.MSG, frag_msg);
                passDown(evt);
            }
            msg.putHeader(getName(), hdr); // GemStoneAddition - save the frag header for rexmits
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.FRAG2_EXCEPTION_IS__0, e);
        }
    }

    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Pass msg up the stack
     */
    void unfragment(Message msg) {
        FragmentationTable frag_table;
        Address            sender=msg.getSrc();
        Message            assembled_msg;
        FragHeader         hdr=(FragHeader)msg.removeHeader(getName());

        synchronized(preJoinMessages) {
          if (this.initialViewId == 0) {
            preJoinMessages.add(msg);
            return;
          }
        }
        
        frag_table=fragment_list.get(sender);
        if(frag_table == null) {
            frag_table=new FragmentationTable(sender);
            try {
                fragment_list.add(sender, frag_table);
            }
            catch(IllegalArgumentException x) { // the entry has already been added, probably in parallel from another thread
                frag_table=fragment_list.get(sender);
            }
        }
        num_received_frags++;
        assembled_msg=frag_table.add(hdr.id, hdr.frag_id, hdr.num_frags, msg);
        if(assembled_msg != null) {
            try {
                if(trace) log.trace("assembled_msg is " + assembled_msg);
                assembled_msg.setSrc(sender); // needed ? YES, because fragments have a null src !!
                num_received_msgs++;
                //this.log.getLogWriter().info("fg size = " + this.fragment_list.size());
                passUp(new Event(Event.MSG, assembled_msg));
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.FRAG2_EXCEPTION_IS__0, e);
            }
        }
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("frag_size")) {
            frag_size=((Integer)map.get("frag_size")).intValue();
            if(log.isDebugEnabled()) log.debug("setting frag_size=" + frag_size);
        }
    }




    /**
     * A fragmentation list keeps a list of fragmentation tables
     * sorted by an Address ( the sender ).
     * This way, if the sender disappears or leaves the group half way
     * sending the content, we can simply remove this members fragmentation
     * table and clean up the memory of the receiver.
     * We do not have to do the same for the sender, since the sender doesn't keep a fragmentation table
     */
    static class FragmentationList  {
        /* * HashMap key=Address, value=FragmentationTable, initialize the hashtable to hold all the fragmentation
         * tables (11 is the best growth capacity to start with)
         */
        private final HashMap frag_tables=new HashMap(11);


        /**
         * Adds a fragmentation table for this particular sender
         * If this sender already has a fragmentation table, an IllegalArgumentException
         * will be thrown.
         * @param   sender - the address of the sender, cannot be null
         * @param   table - the fragmentation table of this sender, cannot be null
         * @exception IllegalArgumentException if an entry for this sender already exist
         */
        public void add(Address sender, FragmentationTable table) throws IllegalArgumentException {
            FragmentationTable healthCheck;

            synchronized(frag_tables) {
                healthCheck=(FragmentationTable)frag_tables.get(sender);
                if(healthCheck == null) {
                    frag_tables.put(sender, table);
                }
                else {
                    throw new IllegalArgumentException("Sender <" + sender + "> already exists in the fragementation list.");
                }
            }
        }

        /**
         * returns a fragmentation table for this sender
         * returns null if the sender doesn't have a fragmentation table
         * @return the fragmentation table for this sender, or null if no table exist
         */
        public FragmentationTable get(Address sender) {
            synchronized(frag_tables) {
                return (FragmentationTable)frag_tables.get(sender);
            }
        }


        /**
         * returns true if this sender already holds a
         * fragmentation for this sender, false otherwise
         * @param sender - the sender, cannot be null
         * @return true if this sender already has a fragmentation table
         */
        public boolean containsSender(Address sender) {
            synchronized(frag_tables) {
                return frag_tables.containsKey(sender);
            }
        }

        /**
         * removes the fragmentation table from the list.
         * after this operation, the fragementation list will no longer
         * hold a reference to this sender's fragmentation table
         * @param sender - the sender who's fragmentation table you wish to remove, cannot be null
         * @return true if the table was removed, false if the sender doesn't have an entry
         */
        public boolean remove(Address sender) {
            synchronized(frag_tables) {
                boolean result=containsSender(sender);
                frag_tables.remove(sender);
                return result;
            }
        }

        /**
         * returns a list of all the senders that have fragmentation tables
         * opened.
         * @return an array of all the senders in the fragmentation list
         */
        public Address[] getSenders() {
            Address[] result;
            int index=0;

            synchronized(frag_tables) {
                result=new Address[frag_tables.size()];
                for(Iterator it=frag_tables.keySet().iterator(); it.hasNext();) {
                    result[index++]=(Address)it.next();
                }
            }
            return result;
        }

        @Override // GemStoneAddition  
        public String toString() {
            Map.Entry entry;
            StringBuffer buf=new StringBuffer("Fragmentation list contains ");
            synchronized(frag_tables) {
                buf.append(frag_tables.size()).append(" tables\n");
                for(Iterator it=frag_tables.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    buf.append(entry.getKey()).append(": " ).append(entry.getValue()).append("\n");
                }
            }
            return buf.toString();
        }
        
        /** GemStoneAddition - sanity check */
        public int size() {
          int result= 0;
          synchronized(frag_tables) {
            for (Iterator it=frag_tables.values().iterator(); it.hasNext(); ) {
              FragmentationTable f = (FragmentationTable)it.next();
              result += f.size();
            }
          }
          return result;
        }

    }

    /**
     * Keeps track of the fragments that are received.
     * Reassembles fragements into entire messages when all fragments have been received.
     * The fragmentation holds a an array of byte arrays for a unique sender
     * The first dimension of the array is the order of the fragmentation, in case the arrive out of order
     */
    static class FragmentationTable  {
        private final Address sender;
        /* the hashtable that holds the fragmentation entries for this sender*/
        private final Hashtable h=new Hashtable(11);  // keys: frag_ids, vals: Entrys


        FragmentationTable(Address sender) {
            this.sender=sender;
        }


        /**
         * inner class represents an entry for a message
         * each entry holds an array of byte arrays sorted
         * once all the byte buffer entries have been filled
         * the fragmentation is considered complete.
         */
        static class Entry  {
            //the total number of fragment in this message
            int tot_frags=0;
            // each fragment is a byte buffer
            Message fragments[]=null;
            //the number of fragments we have received
            int number_of_frags_recvd=0;
            // the message ID
            long msg_id=-1;

            /**
             * Creates a new entry
             * @param tot_frags the number of fragments to expect for this message
             */
            Entry(long msg_id, int tot_frags) {
                this.msg_id=msg_id;
                this.tot_frags=tot_frags;
                fragments=new Message[tot_frags];
                for(int i=0; i < tot_frags; i++)
                    fragments[i]=null;
            }

            /**
             * adds on fragmentation buffer to the message
             * @param frag_id the number of the fragment being added 0..(tot_num_of_frags - 1)
             * @param frag the byte buffer containing the data for this fragmentation, should not be null
             */
            public void set(int frag_id, Message frag) {
                 // don't count an already received fragment (should not happen though because the
                // reliable transmission protocol(s) below should weed out duplicates
                if(fragments[frag_id] == null) {
                    fragments[frag_id]=frag;
                    number_of_frags_recvd++;
                }
            }

            /** returns true if this fragmentation is complete
             *  ie, all fragmentations have been received for this buffer
             *
             */
            public boolean isComplete() {
                /*first make the simple check*/
                if(number_of_frags_recvd < tot_frags) {
                    return false;
                }
                /*then double check just in case*/
                for(int i=0; i < fragments.length; i++) {
                    if(fragments[i] == null)
                        return false;
                }
                /*all fragmentations have been received*/
                return true;
            }

            /**
             * Assembles all the fragments into one buffer. Takes all Messages, and combines their buffers into one
             * buffer.
             * This method does not check if the fragmentation is complete (use {@link #isComplete()} to verify
             * before calling this method)
             * @return the complete message in one buffer
             *
             */
            public Message assembleMessage() {
                Message retval;
                byte[]  combined_buffer, tmp;
                int     combined_length=0, length, offset;
                Message fragment;
                int     index=0;

                for(int i=0; i < fragments.length; i++) {
                    fragment=fragments[i];
                    combined_length+=fragment.getLength();
                }

                combined_buffer=new byte[combined_length];
                for(int i=0; i < fragments.length; i++) {
                    fragment=fragments[i];
                    tmp=fragment.getRawBuffer();
                    length=fragment.getLength();
                    offset=fragment.getOffset();
                    System.arraycopy(tmp, offset, combined_buffer, index, length);
                    index+=length;
                }

                retval=fragments[fragments.length-1].copy(false); // GemStoneAddition - for DirAck support we copy the final fragment
                retval.setBuffer(combined_buffer);
                return retval;
            }

            /**
             * debug only
             */
            @Override // GemStoneAddition  
            public String toString() {
                StringBuffer ret=new StringBuffer();
                ret.append("[tot_frags=" + tot_frags + ", number_of_frags_recvd=" + number_of_frags_recvd + ']');
                return ret.toString();
            }

            @Override // GemStoneAddition  
            public int hashCode() {
                return super.hashCode();
            }
            
            /** GemStoneAddition - debugging memory */
            public int size() {
              int result = 0;
              for (int i=0; i<fragments.length; i++) {
                Message m = fragments[i];
                if (m != null) {
                  result += m.getLength();
                }
              }
              return result;
            }
        }


        /**
         * Creates a new entry if not yet present. Adds the fragment.
         * If all fragements for a given message have been received,
         * an entire message is reassembled and returned.
         * Otherwise null is returned.
         * @param   id - the message ID, unique for a sender
         * @param   frag_id the index of this fragmentation (0..tot_frags-1)
         * @param   tot_frags the total number of fragmentations expected
         * @param   fragment - the byte buffer for this fragment
         */
        public synchronized Message add(long id, int frag_id, int tot_frags, Message fragment) {
            Message retval=null;

            Entry e=(Entry)h.get(Long.valueOf(id));

            if(e == null) {   // Create new entry if not yet present
                e=new Entry(id, tot_frags);
                h.put(Long.valueOf(id), e);
            }

            e.set(frag_id, fragment);
            if(e.isComplete()) {
                retval=e.assembleMessage();
                h.remove(Long.valueOf(id));
            }

            return retval;
        }


        public void reset() {
        }

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer buf=new StringBuffer("Fragmentation Table Sender:").append(sender).append("\n\t");
            java.util.Enumeration e=this.h.elements();
            while(e.hasMoreElements()) {
                Entry entry=(Entry)e.nextElement();
                int count=0;
                for(int i=0; i < entry.fragments.length; i++) {
                    if(entry.fragments[i] != null) {
                        count++;
                    }
                }
                buf.append("Message ID:").append(entry.msg_id).append("\n\t");
                buf.append("Total Frags:").append(entry.tot_frags).append("\n\t");
                buf.append("Frags Received:").append(count).append("\n\n");
            }
            return buf.toString();
        }
        
        /** GemStoneAddition - memory debugging */
        public int size() {
          int result = 0;
          java.util.Enumeration e = h.elements();
          while (e.hasMoreElements()) {
            Entry entry = (Entry)e.nextElement();
            result += entry.size();
          }
          return result;
        }
    }

}


