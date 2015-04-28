/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Event.java,v 1.12 2005/12/08 12:53:53 belaban Exp $

package com.gemstone.org.jgroups;



/**
 * Used for inter-stack and intra-stack communication.
 * @author Bela Ban
 */
public class Event {
    public static final int MSG                       =  1;  // arg = Message
    public static final int CONNECT                   =  2;  // arg = group address (string)
    public static final int CONNECT_OK                =  3;  // arg = group multicast address (Address)
    public static final int DISCONNECT                =  4;  // arg = member address (Address)
    public static final int DISCONNECT_OK             =  5;
    public static final int VIEW_CHANGE               =  6;  // arg = View (or MergeView in case of merge)
    public static final int GET_LOCAL_ADDRESS         =  7;
    public static final int SET_LOCAL_ADDRESS         =  8;
    public static final int SUSPECT                   =  9;  // arg = Address of suspected member
    public static final int BLOCK                     = 10;
    public static final int BLOCK_OK                  = 11;
    public static final int FIND_INITIAL_MBRS         = 12;
    public static final int FIND_INITIAL_MBRS_OK      = 13;  // arg = Vector of PingRsps
    public static final int MERGE                     = 14;  // arg = Vector of Objects
    public static final int TMP_VIEW                  = 15;  // arg = View
    public static final int BECOME_SERVER             = 16;  // sent when client has joined group
    public static final int GET_APPLSTATE             = 17;  // get state from appl (arg=requestor)
    public static final int GET_APPLSTATE_OK          = 18;  // arg = byte[] (state)
    public static final int GET_STATE                 = 19;  // arg = StateTransferInfo
    public static final int GET_STATE_OK              = 20;  // arg = Object or Vector (state(s))
    public static final int STATE_RECEIVED            = 21;  // arg = state
    public static final int START_QUEUEING            = 22;
    public static final int STOP_QUEUEING             = 23;  // arg = Vector (event-list)
    public static final int SWITCH_NAK                = 24;
    public static final int SWITCH_NAK_ACK            = 25;
    public static final int SWITCH_OUT_OF_BAND        = 26;
    public static final int FLUSH                     = 27;  // arg = Vector (destinatinon for FLUSH)
    public static final int FLUSH_OK                  = 28;  // arg = FlushRsp
    public static final int DROP_NEXT_MSG             = 29;
    public static final int STABLE                    = 30;  // arg = long[] (stable seqnos for mbrs)
    public static final int GET_MSG_DIGEST            = 31;  // arg = long[] (highest seqnos from mbrs)
    public static final int GET_MSG_DIGEST_OK         = 32;  // arg = Digest
    public static final int REBROADCAST_MSGS          = 33;  // arg = Vector (msgs with NakAckHeader)
    public static final int REBROADCAST_MSGS_OK       = 34;
    public static final int GET_MSGS_RECEIVED         = 35;
    public static final int GET_MSGS_RECEIVED_OK      = 36;  // arg = long[] (highest deliverable seqnos)
    public static final int GET_MSGS                  = 37;  // arg = long[][] (range of seqnos for each m.)
    public static final int GET_MSGS_OK               = 38;  // arg = List
    public static final int GET_DIGEST                = 39;  //
    public static final int GET_DIGEST_OK             = 40;  // arg = Digest (response to GET_DIGEST)
    public static final int SET_DIGEST                = 41;  // arg = Digest
    public static final int GET_DIGEST_STATE          = 42;  // see ./JavaStack/Protocols/pbcast/DESIGN for explanantion
    public static final int GET_DIGEST_STATE_OK       = 43;  // see ./JavaStack/Protocols/pbcast/DESIGN for explanantion
    public static final int SET_PARTITIONS            = 44;  // arg = Hashtable of addresses and numbers
    public static final int MERGE_DENIED              = 45;  // Passed down from gms when a merge attempt fails
    public static final int EXIT                      = 46;  // received when member was forced out of the group
    public static final int PERF                      = 47;  // for performance measurements
    public static final int SUBVIEW_MERGE             = 48;  // arg = vector of addresses; see JGroups/EVS/Readme.txt
    public static final int SUBVIEWSET_MERGE          = 49;  // arg = vector of addresses; see JGroups/EVS/Readme.txt
    public static final int HEARD_FROM                = 50;  // arg = Vector (list of Addresses)
    public static final int UNSUSPECT                 = 51;  // arg = Address (of unsuspected member)
    public static final int SET_PID                   = 52;  // arg = Integer (process id)
    public static final int MERGE_DIGEST              = 53;  // arg = Digest
    public static final int BLOCK_SEND                = 54;  // arg = null
    public static final int UNBLOCK_SEND              = 55;  // arg = null
    public static final int CONFIG                    = 56;  // arg = HashMap (config properties)
    public static final int GET_DIGEST_STABLE         = 57;
    public static final int GET_DIGEST_STABLE_OK      = 58;  // response to GET_DIGEST_STABLE
    // public static final int ACK                       = 59;  // used to flush down events
    // public static final int ACK_OK                    = 60;  // response to ACK
    public static final int START                     = 61;  // triggers start() - internal event, handled by Protocol
    public static final int START_OK                  = 62;  // arg = exception of null - internal event, handled by Protocol
    public static final int STOP                      = 63;  // triggers stop() - internal event, handled by Protocol
    public static final int STOP_OK                   = 64;  // arg = exception or null - internal event, handled by Protocol
    public static final int SUSPEND_STABLE            = 65;  // arg = Long (max_suspend_time)
    public static final int RESUME_STABLE             = 66;  // arg = null
    public static final int ENABLE_UNICASTS_TO        = 67;  // arg = Address (member)



    public static final int USER_DEFINED=1000;// arg = <user def., e.g. evt type + data>
    public static final int SET_MCAST_ADDRESS         = 1001; // arg = Address (mcast address) GemStoneAddition
    public static final int ENABLE_INITIAL_COORDINATOR = 1002; // tells gms it can become initial coord - GemStoneAddition
    public static final int UNUSED1 = 1003;
    public static final int FLOATING_COORDINATOR_DISABLED = 1004; // network partitioning detection - GemStoneAddition
    public static final int GMS_SUSPECT = 1005; // GMS initiated suspicion - failed view ackers - GemStoneAddition
    public static final int SUSPECT_NOT_MEMBER = 1006; // FD_SOCK initiated event - suspected mbr is not in view - GemStoneAddition
    public static final int DISCONNECTING = 1007; // GMS initiating a disconnect - GemStoneAddition
    public static final int DISABLE_UNICASTS_TO = 1008; // Rejected an attempt to join, so remove from UNICAST receivers
    public static final int FD_SOCK_MEMBER_LEFT_NORMALLY = 1009; // connection to coordinator terminated normally in FD_SOCK
    public static final int ENABLE_NETWORK_PARTITION_DETECTION = 1010; // turn on network partition detection
    public static final int FIND_INITIAL_MBRS_FAILED = 1011; // required members didn't respond - arg contains missing addresses


    private int    type;       // type of event
    private Object arg;        // must be serializable if used for inter-stack communication


    public Event(int type) {
        this.type=type;
    }

    public Event(int type, Object arg) {
        this.type=type;
        this.arg=arg;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type=type;
    }

    public <T> T getArg() {
        return (T)arg;
    }

    public void setArg(Object arg) {
        this.arg=arg;
    }



    public static String type2String(int t) {
        switch(t) {
            case MSG:	                 return "MSG";
            case CONNECT:	             return "CONNECT";
            case CONNECT_OK:	         return "CONNECT_OK";
            case DISCONNECT:	         return "DISCONNECT";
            case DISCONNECT_OK: 	     return "DISCONNECT_OK";
            case VIEW_CHANGE:	         return "VIEW_CHANGE";
            case GET_LOCAL_ADDRESS:	     return "GET_LOCAL_ADDRESS";
            case SET_LOCAL_ADDRESS:	     return "SET_LOCAL_ADDRESS";
            case SUSPECT:                return "SUSPECT";
            case BLOCK:	                 return "BLOCK";
            case BLOCK_OK:               return "BLOCK_OK";
            case FIND_INITIAL_MBRS:	     return "FIND_INITIAL_MBRS";
            case FIND_INITIAL_MBRS_OK:   return "FIND_INITIAL_MBRS_OK";
            case TMP_VIEW:	             return "TMP_VIEW";
            case BECOME_SERVER:	         return "BECOME_SERVER";
            case GET_APPLSTATE:          return "GET_APPLSTATE";
            case GET_APPLSTATE_OK:       return "GET_APPLSTATE_OK";
            case GET_STATE:              return "GET_STATE";
            case GET_STATE_OK:           return "GET_STATE_OK";
            case STATE_RECEIVED:         return "STATE_RECEIVED";
            case START_QUEUEING:         return "START_QUEUEING";
            case STOP_QUEUEING:          return "STOP_QUEUEING";
            case SWITCH_NAK:             return "SWITCH_NAK";
            case SWITCH_NAK_ACK:         return "SWITCH_NAK_ACK";
            case SWITCH_OUT_OF_BAND:     return "SWITCH_OUT_OF_BAND";
            case FLUSH:                  return "FLUSH";
            case FLUSH_OK:               return "FLUSH_OK";
            case DROP_NEXT_MSG:          return "DROP_NEXT_MSG";
            case STABLE:                 return "STABLE";
            case GET_MSG_DIGEST:         return "GET_MSG_DIGEST";
            case GET_MSG_DIGEST_OK:      return "GET_MSG_DIGEST_OK";
            case REBROADCAST_MSGS:       return "REBROADCAST_MSGS";
            case REBROADCAST_MSGS_OK:    return "REBROADCAST_MSGS_OK";
            case GET_MSGS_RECEIVED:      return "GET_MSGS_RECEIVED";
            case GET_MSGS_RECEIVED_OK:   return "GET_MSGS_RECEIVED_OK";
            case GET_MSGS:               return "GET_MSGS";
            case GET_MSGS_OK:            return "GET_MSGS_OK";
            case GET_DIGEST:             return "GET_DIGEST";
            case GET_DIGEST_OK:          return "GET_DIGEST_OK";
            case SET_DIGEST:             return "SET_DIGEST";
            case GET_DIGEST_STATE:       return "GET_DIGEST_STATE";
            case GET_DIGEST_STATE_OK:    return "GET_DIGEST_STATE_OK";
            case SET_PARTITIONS:         return "SET_PARTITIONS"; // Added by gianlucac@tin.it to support PARTITIONER
            case MERGE:                  return "MERGE"; // Added by gianlucac@tin.it to support partitions merging in GMS
            case MERGE_DENIED:           return "MERGE_DENIED";// as above
            case EXIT:                   return "EXIT";
            case PERF:                   return "PERF";
            case SUBVIEW_MERGE:          return "SUBVIEW_MERGE";
            case SUBVIEWSET_MERGE:       return "SUBVIEWSET_MERGE";
            case HEARD_FROM:             return "HEARD_FROM";
            case UNSUSPECT:              return "UNSUSPECT";
            case SET_PID:                return "SET_PID";
            case MERGE_DIGEST:           return "MERGE_DIGEST";
            case BLOCK_SEND:             return "BLOCK_SEND";
            case UNBLOCK_SEND:           return "UNBLOCK_SEND";
            case CONFIG:                 return "CONFIG";
            case GET_DIGEST_STABLE:      return "GET_DIGEST_STABLE";
            case GET_DIGEST_STABLE_OK:   return "GET_DIGEST_STABLE_OK";
            // case ACK:                    return "ACK";
            // case ACK_OK:                 return "ACK_OK";
            case START:                  return "START";
            case START_OK:               return "START_OK";
            case STOP:                   return "STOP";
            case STOP_OK:                return "STOP_OK";
            case SUSPEND_STABLE:         return "SUSPEND_STABLE";
            case RESUME_STABLE:          return "RESUME_STABLE";
            case ENABLE_UNICASTS_TO:     return "ENABLE_UNICASTS_TO";

            case USER_DEFINED:           return "USER_DEFINED";
            // GemStoneAddition - new event types
            case SET_MCAST_ADDRESS:      return "SET_MCAST_ADDRESS";
            case ENABLE_INITIAL_COORDINATOR: return "ENABLE_INITIAL_COORDINATOR";
            case FLOATING_COORDINATOR_DISABLED: return "FLOATING_COORDINATOR_DISABLED";
            case GMS_SUSPECT: return "GMS_SUSPECT";
            case ENABLE_NETWORK_PARTITION_DETECTION: return "ENABLE_NETWORK_PARTITION_DETECTION";
            case FIND_INITIAL_MBRS_FAILED: return "FIND_INITIAL_MBRS_FAILED";
            
            default:                     return "UNDEFINED";
        }
    }

    public static final Event FIND_INITIAL_MBRS_EVT = new Event(Event.FIND_INITIAL_MBRS);
    public static final Event GET_DIGEST_EVT        = new Event(Event.GET_DIGEST);

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer(64);
        ret.append("Event[type=" + type2String(type) + ", arg=" + arg + ']');
        return ret.toString();
    }

}

