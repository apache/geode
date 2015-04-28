/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PARTITIONER.java,v 1.5 2005/05/30 14:31:07 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;



/**
 * This layer can be put on top of the bottommost layer and is useful to simulate partitions.
 * It simply adds a header with its partition number and discards Messages with other partition numbers.<br>
 * If it receives an Event of type Event.SET_PARTITIONS it sends a Header of type COMMAND with the Hashtable
 * contained in the Event argument to set the partitions of ALL processes (not just processes of the current view but
 * every process with the same group address that receives the message.
 */

public class PARTITIONER extends Protocol  {
    final Vector   members=new Vector();
    Address  local_addr=null;
    int      my_partition=1;

    /** All protocol names have to be unique ! */
    @Override // GemStoneAddition  
    public String  getName() {return "PARTITIONER";}


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
//        String     str; GemStoneAddition

        super.setProperties(props);
        if(props.size() > 0) {
            log.error(ExternalStrings.PARTITIONER_EXAMPLESETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    /** Just remove if you don't need to reset any state */
    public void reset() {}


    /**
     * Discards Messages with the wrong partition number and sets local partition number if
     * it receives a COMMAND Header
     */

    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        Integer num;
        PartitionerHeader partHead=null;

        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
	    local_addr=(Address) evt.getArg();
	     if(log.isInfoEnabled()) log.info(ExternalStrings.PARTITIONER_LOCAL_ADDRESS_IS__0, local_addr);
	    break;

        case Event.MSG:
            msg=(Message)evt.getArg();
            partHead=(PartitionerHeader) msg.removeHeader(getName());
            if (partHead.type == PartitionerHeader.COMMAND) {
		num = (Integer) partHead.Destinations.get(local_addr);
		if (num == null) return;
		 if(log.isInfoEnabled()) log.info(ExternalStrings.PARTITIONER_NEW_PARTITION___0, num);
		my_partition =num.intValue();
		return;
            }
            if (partHead.type == PartitionerHeader.NORMAL && partHead.partition != my_partition ) return;
            break;
        }

        passUp(evt);            // Pass up to the layer above us
    }




    /**
     * Adds to Messages a Header with the local partitin number and if receives a SET_PARTITIONS Event sends
     * a new Message with a PartitionerHeader set to COMMAND that carries the Hashtable
     */

    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message msg;
//        Event newEvent; GemStoneAddition
        PartitionerHeader partHeader;

        switch(evt.getType()) {

        case Event.SET_PARTITIONS:
	    //Sends a partitioning message
	     if(log.isInfoEnabled()) log.info(ExternalStrings.PARTITIONER_SET_PARTITIONS_RECEIVED_ARGUMENT__0, evt.getArg().toString());
	    msg = new Message(null,null,null);
	    partHeader = new PartitionerHeader(PartitionerHeader.COMMAND);
	    partHeader.Destinations = (Hashtable) evt.getArg();
	    msg.putHeader(getName(), partHeader);
	    passDown(new Event(Event.MSG,msg));
	    break;

        case Event.MSG:
            msg=(Message)evt.getArg();
            msg.putHeader(getName(), new PartitionerHeader(PartitionerHeader.NORMAL,my_partition));
            // Do something with the event, e.g. add a header to the message
            // Optionally pass down
            break;
        }

        passDown(evt);          // Pass on to the layer below us
    }





/**
 * The Partitioner header normally (type = NORMAL) contains just the partition number that is checked to discard messages
 * received from other partitions.
 * If type is COMMAND Destination contains an Hashtable where keys are of type Address and represent process (channel)
 * addresses and values are Integer representing the partition that shuold be assigned to each Address.
 */

    public static class PartitionerHeader extends Header  {
	// your variables
	static final int NORMAL=0; //normal header (do nothing)
	static final int COMMAND=1; //set partition vector
	int type=0,partition=1;
	Hashtable Destinations=null;
	
	public PartitionerHeader () {} // used for externalization
	public PartitionerHeader (int type) { this.type= type;    }
	public PartitionerHeader (int type,int partition) { this.type= type; this.partition = partition;  }
	
	    @Override // GemStoneAddition  
	public String toString() {
	    switch (type) {
	    case NORMAL: return "NORMAL ->partition :" + partition;
	    case COMMAND: return "COMMAND ->hashtable :" + Destinations;
	    default: return "<unknown>";
		
	    }
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeInt(type);
	    out.writeInt(partition);
	    out.writeObject(Destinations);
	}
	
	
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    type=in.readInt();
	    partition=in.readInt();
	    Destinations=(Hashtable)in.readObject();
	}
}
    



}
