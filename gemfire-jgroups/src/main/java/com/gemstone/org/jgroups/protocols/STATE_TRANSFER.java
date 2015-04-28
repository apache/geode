/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: STATE_TRANSFER.java,v 1.16 2005/11/12 06:38:21 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.blocks.RequestCorrelator;
import com.gemstone.org.jgroups.blocks.RequestHandler;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.StateTransferInfo;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Rsp;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;


class StateTransferRequest implements Serializable {
    static final int MAKE_COPY=1;  // arg = originator of request
    static final int RETURN_STATE=2;  // arg = orginator of request

    int type=0;
    final Object arg;
    private static final long serialVersionUID = -7734608266762273116L;


    StateTransferRequest(int type, Object arg) {
        this.type=type;
        this.arg=arg;
    }

    public int getType() {
        return type;
    }

    public Object getArg() {
        return arg;
    }

    @Override // GemStoneAddition  
    public String toString() {
        return "[StateTransferRequest: type=" + type2Str(type) + ", arg=" + arg + ']';
    }

    static String type2Str(int t) {
        switch(t) {
            case MAKE_COPY:
                return "MAKE_COPY";
            case RETURN_STATE:
                return "RETURN_STATE";
            default:
                return "<unknown>";
        }
    }
}


/**
 * State transfer layer. Upon receiving a GET_STATE event from JChannel, a MAKE_COPY message is
 * sent to all members. When the originator receives MAKE_COPY, it queues all messages to the
 * channel.
 * When another member receives the message, it asks the JChannel to provide it with a copy of
 * the current state (GetStateEvent is received by application, returnState() sends state down the
 * stack). Then the current layer sends a unicast RETURN_STATE message to the coordinator, which
 * returns the cached copy.
 * When the state is received by the originator, the GET_STATE sender is unblocked with a
 * GET_STATE_OK event up the stack (unless it already timed out).<p>
 * Requires QUEUE layer on top.
 * 
 * @author Bela Ban
 */
public class STATE_TRANSFER extends Protocol implements RequestHandler {
    Address local_addr=null;
    final Vector members=new Vector(11);
    final Message m=null;
    boolean is_server=false;
    byte[] cached_state=null;
    final Object state_xfer_mutex=new Object(); // get state from appl (via channel).
    long timeout_get_appl_state=5000;
    long timeout_return_state=5000;
    RequestCorrelator corr=null;
    final Vector observers=new Vector(5);
    final HashMap map=new HashMap(7);


    /**
     * All protocol names have to be unique !
     */
    @Override // GemStoneAddition  
    public String getName() {
        return "STATE_TRANSFER";
    }


    @Override // GemStoneAddition  
    public void init() throws Exception {
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());

    }

    @Override // GemStoneAddition  
    public void start() throws Exception {
        corr=new RequestCorrelator(getName(), this, this);
        passUp(new Event(Event.CONFIG, map));
    }

    @Override // GemStoneAddition  
    public void stop() {
        if(corr != null) {
            corr.stop();
            corr=null;
        }
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        // Milliseconds to wait for application to provide requested state, events are
        // STATE_TRANSFER up and STATE_TRANSFER_OK down
        str=props.getProperty("timeout_get_appl_state");
        if(str != null) {
            timeout_get_appl_state=Long.parseLong(str);
            props.remove("timeout_get_appl_state");
        }

        // Milliseconds to wait for 1 or all members to return its/their state. 0 means wait
        // forever. States are retrieved using GroupRequest/RequestCorrelator
        str=props.getProperty("timeout_return_state");
        if(str != null) {
            timeout_return_state=Long.parseLong(str);
            props.remove("timeout_return_state");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.STATE_TRANSFER_STATE_TRANSFERSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public Vector requiredUpServices() {
        Vector ret=new Vector(2);
        ret.addElement(Integer.valueOf(Event.START_QUEUEING));
        ret.addElement(Integer.valueOf(Event.STOP_QUEUEING));
        return ret;
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        switch(evt.getType()) {

            case Event.BECOME_SERVER:
                is_server=true;
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Vector new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.removeAllElements();
                    if(new_members != null && new_members.size() > 0)
                        for(int k=0; k < new_members.size(); k++)
                            members.addElement(new_members.elementAt(k));
                }
                break;
        }

        if(corr != null)
            corr.receive(evt); // will consume or pass up, depending on header
        else
            passUp(evt);
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Object coord, state;
        Vector event_list;
        StateTransferInfo info;


        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Vector new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.removeAllElements();
                    if(new_members != null && new_members.size() > 0)
                        for(int k=0; k < new_members.size(); k++)
                            members.addElement(new_members.elementAt(k));
                }
                break;

            case Event.GET_STATE:       // generated by JChannel.getState()
                info=(StateTransferInfo)evt.getArg();
                coord=determineCoordinator();

                if(coord == null || coord.equals(local_addr)) {
                    event_list=new Vector(1);
                    event_list.addElement(new Event(Event.GET_STATE_OK, null));
                    passUp(new Event(Event.STOP_QUEUEING, event_list));
                    return;             // don't pass down any further !
                }

                sendMakeCopyMessage();  // multicast MAKE_COPY to all members (including me)

                if(info.type == StateTransferInfo.GET_FROM_MANY)
                    state=getStateFromMany(info.targets);
                else
                    state=getStateFromSingle(info.target);

                /* Pass up the state to the application layer (insert into JChannel's event queue */
                event_list=new Vector(1);
                event_list.addElement(new Event(Event.GET_STATE_OK, state));

                /* Now stop queueing */
                passUp(new Event(Event.STOP_QUEUEING, event_list));
                return;                 // don't pass down any further !

            case Event.GET_APPLSTATE_OK:
                synchronized(state_xfer_mutex) {
                    cached_state=(byte[])evt.getArg();
                    state_xfer_mutex.notifyAll();
                }
                return;                 // don't pass down any further !

        }

        passDown(evt);              // pass on to the layer below us
    }


    /* ---------------------- Interface RequestHandler -------------------------- */
    public Object handle(Message msg) {
        StateTransferRequest req;

        try {
            req=(StateTransferRequest)msg.getObject();

            switch(req.getType()) {
                case StateTransferRequest.MAKE_COPY:
                    makeCopy(req.getArg());
                    return null;
                case StateTransferRequest.RETURN_STATE:
                    if(is_server)
                        return cached_state;
                    else {
                        if(warn) log.warn("RETURN_STATE: returning null" +
                                "as I'm not yet an operational state server !");
                        return null;
                    }
                default:
                    if(log.isErrorEnabled()) log.error("type " + req.getType() +
                            "is unknown in StateTransferRequest !");
                    return null;
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.STATE_TRANSFER_EXCEPTION_IS__0, e);
            return null;
        }
    }
    /* ------------------- End of Interface RequestHandler ---------------------- */








    byte[] getStateFromSingle(Address target) {
        Vector dests=new Vector(11);
        Message msg;
        StateTransferRequest r=new StateTransferRequest(StateTransferRequest.RETURN_STATE, local_addr);
        RspList rsp_list;
        Rsp rsp;
        Address dest;
        GroupRequest req;
        int num_tries=0;


        try {
            msg=new Message(null, null, Util.objectToByteBuffer(r));
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.STATE_TRANSFER_EXCEPTION_0, e);
            return null;
        }

        while(members.size() > 1 && num_tries++ < 3) {  // excluding myself
            dest=target != null? target : determineCoordinator();
            if(dest == null)
                return null;
            msg.setDest(dest);
            dests.removeAllElements();
            dests.addElement(dest);
            req=new GroupRequest(msg, corr, dests, GroupRequest.GET_FIRST, timeout_return_state, 0);
            req.execute();
            rsp_list=req.getResults();
            for(int i=0; i < rsp_list.size(); i++) {  // get the first non-suspected result
                rsp=(Rsp)rsp_list.elementAt(i);
                if(rsp.wasReceived())
                    return (byte[])rsp.getValue();
            }
            try { // GemStoneAddition
              Util.sleep(1000);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return null; // treat like it failed
            }
        }

        return null;
    }


    Vector getStateFromMany(Vector targets) {
        Vector dests=new Vector(11);
        Message msg;
        StateTransferRequest r=new StateTransferRequest(StateTransferRequest.RETURN_STATE, local_addr);
        RspList rsp_list;
        GroupRequest req;
        int i;


        if(targets != null) {
            for(i=0; i < targets.size(); i++)
                if(!local_addr.equals(targets.elementAt(i)))
                    dests.addElement(targets.elementAt(i));
        }
        else {
            for(i=0; i < members.size(); i++)
                if(!local_addr.equals(members.elementAt(i)))
                    dests.addElement(members.elementAt(i));
        }

        if(dests.size() == 0)
            return null;

        msg=new Message();
        try {
            msg.setBuffer(Util.objectToByteBuffer(r));
        }
        catch(Exception e) {
        }

        req=new GroupRequest(msg, corr, dests, GroupRequest.GET_ALL, timeout_return_state, 0);
        req.execute();
        rsp_list=req.getResults();
        return rsp_list.getResults();
    }


    void sendMakeCopyMessage() {
        GroupRequest req;
        Message msg=new Message();
        StateTransferRequest r=new StateTransferRequest(StateTransferRequest.MAKE_COPY, local_addr);
        Vector dests=new Vector(11);

        for(int i=0; i < members.size(); i++)   
             dests.addElement(members.elementAt(i));

        if(dests.size() == 0)
            return;

        try {
            msg.setBuffer(Util.objectToByteBuffer(r));
        }
        catch(Exception e) {
        }

        req=new GroupRequest(msg, corr, dests, GroupRequest.GET_ALL, timeout_return_state, 0);
        req.execute();
    }


    /**
     * Return the first element of members which is not me. Otherwise return null.
     */
    Address determineCoordinator() {
        Address ret=null;
        if(members != null && members.size() > 1) {
            for(int i=0; i < members.size(); i++)
                if(!local_addr.equals(members.elementAt(i)))
                    return (Address)members.elementAt(i);
        }
        return ret;
    }


    /**
     * If server, ask application to send us a copy of its state (STATE_TRANSFER up,
     * STATE_TRANSFER down). If client, start queueing events. Queuing will be stopped when
     * state has been retrieved (or not) from single or all member(s).
     */
    void makeCopy(Object sender) {
        if(sender.equals(local_addr)) { // was sent by us, has to start queueing
            passUp(new Event(Event.START_QUEUEING));
        }
        else {               // only retrieve state from appl when not in client state anymore
            if(is_server) {  // get state from application and store it locally
                synchronized(state_xfer_mutex) {
                    cached_state=null;

                    passUp(new Event(Event.GET_APPLSTATE, local_addr));
                    if(cached_state == null) {
                        try {
                            state_xfer_mutex.wait(timeout_get_appl_state); // wait for STATE_TRANSFER_OK
                        }
                        catch(InterruptedException e) {
                          Thread.currentThread().interrupt(); // GemStoneAddition
                          // Just propagate to caller
                        }
                    }
                }
            }
        }
    }


}
