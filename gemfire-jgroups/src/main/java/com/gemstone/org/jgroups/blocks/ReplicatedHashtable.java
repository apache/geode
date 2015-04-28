/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReplicatedHashtable.java,v 1.12 2005/11/10 20:54:01 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.*;

// @todo implement putAll() [similar to DistributedHashtable].
/**
 * Provides the abstraction of a java.util.Hashtable that is replicated at several
 * locations. Any change to the hashtable (clear, put, remove etc) will transparently be
 * propagated to all replicas in the group. All read-only methods will always access the
 * local replica.<p>
 * Both keys and values added to the hashtable <em>must be serializable</em>, the reason
 * being that they will be sent across the network to all replicas of the group. Having said
 * this, it is now for example possible to add RMI remote objects to the hashtable as they
 * are derived from <code>java.rmi.server.RemoteObject</code> which in turn is serializable.
 * This allows to lookup shared distributed objects by their name and invoke methods on them,
 * regardless of one's onw location. A <code>ReplicatedHashtable</code> thus allows to
 * implement a distributed naming service in just a couple of lines.<p>
 * An instance of this class will contact an existing member of the group to fetch its
 * initial state.<p>
 * Contrary to DistributedHashtable, this class does not make use of RpcDispatcher (and RequestCorrelator)
 * but uses plain asynchronous messages instead.
 * @author Bela Ban
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
public class ReplicatedHashtable extends Hashtable implements MessageListener, MembershipListener {
    private static final long serialVersionUID = -892359995977718412L;

    public interface Notification {
        void entrySet(Object key, Object value);

        void entryRemoved(Object key);

        void viewChange(Vector new_mbrs, Vector old_mbrs);

        void contentsSet(Map new_entries);
    }

    public interface StateTransferListener {
        void stateTransferStarted();

        void stateTransferCompleted(boolean success);
    }

    transient Channel channel;
    transient PullPushAdapter adapter=null;
    final transient Vector notifs=new Vector();
    // to be notified when mbrship changes
    final transient Vector members=new Vector(); // keeps track of all DHTs
    final transient List state_transfer_listeners=new ArrayList();
    transient boolean state_transfer_running=false;

    /** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
    private transient boolean send_message=false;

    protected final transient GemFireTracer log=GemFireTracer.getLog(this.getClass());

    /**
     * Creates a ReplicatedHashtable
     * @param groupname The name of the group to join
     * @param factory The ChannelFactory which will be used to create a channel
     * @param properties The property string to be used to define the channel
     * @param state_timeout The time to wait until state is retrieved in milliseconds. A value of 0 means wait forever.
     */
    public ReplicatedHashtable(String groupname, ChannelFactory factory, StateTransferListener l, String properties, long state_timeout) {
        if(l != null)
            addStateTransferListener(l);
        try {
            channel=factory != null ? factory.createChannel(properties) : new JChannel(properties);
            channel.connect(groupname);
            adapter=new PullPushAdapter(channel, this, this);
            adapter.setListener(this);
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            getInitState(channel, state_timeout);
//            boolean rc=channel.getState(null, state_timeout);
//            if(rc)
//                if(log.isInfoEnabled()) log.info("state was retrieved successfully");
//            else
//                if(log.isInfoEnabled()) log.info("state could not be retrieved (first member)");
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedHashtable_EXCEPTION_0, e);
        }
    }

    private void getInitState(Channel channel, long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        try {
            notifyStateTransferStarted();
            boolean rc=channel.getState(null, state_timeout);
            if(rc)
                if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedHashtable_STATE_WAS_RETRIEVED_SUCCESSFULLY);
            else {
                if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedHashtable_STATE_COULD_NOT_BE_RETRIEVED_FIRST_MEMBER);
                notifyStateTransferCompleted(false);
            }
        }
        catch(ChannelClosedException ex) {
            notifyStateTransferCompleted(false);
            throw ex;
        }
        catch(ChannelNotConnectedException ex2) {
            notifyStateTransferCompleted(false);
            throw ex2;
        }
    }

    public ReplicatedHashtable(String groupname, ChannelFactory factory, String properties, long state_timeout) {
        this(groupname, factory, null, properties, state_timeout);
    }

    public ReplicatedHashtable(JChannel channel, long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        this(channel, null, state_timeout);
    }

    public ReplicatedHashtable(JChannel channel, StateTransferListener l, long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        this.channel=channel;
        this.adapter=new PullPushAdapter(channel, this, this);
        this.adapter.setListener(this);
        if(l != null)
            addStateTransferListener(l);
        this.channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        getInitState(channel, state_timeout);
//        boolean rc=channel.getState(null, state_timeout);
//        if(rc)
//            if(log.isInfoEnabled()) log.info("state was retrieved successfully");
//        else
//            if(log.isInfoEnabled()) log.info("state could not be retrieved (first member)");
    }

    public boolean stateTransferRunning() {
        return state_transfer_running;
    }

    public Address getLocalAddress() {
        return channel != null ? channel.getLocalAddress() : null;
    }

    public Channel getChannel() {
        return channel;
    }

    public void addNotifier(Notification n) {
        if(!notifs.contains(n))
            notifs.addElement(n);
    }

    public void addStateTransferListener(StateTransferListener l) {
        if(l != null && !(state_transfer_listeners.contains(l)))
            state_transfer_listeners.add(l);
    }

    public void removeStateTransferListener(StateTransferListener l) {
        if(l != null)
            state_transfer_listeners.remove(l);
    }

    /**
     * Maps the specified key to the specified value in the hashtable. Neither of both parameters can be null
     * @param key - the hashtable key
     * @param value - the value
     * @return the previous value of the specified key in this hashtable, or null if it did not have one
     */
    @Override // GemStoneAddition
    public Object put(Object key, Object value) {
        Message msg;
        Object prev_val=null;
        prev_val=get(key);

        //Changes done by <aos>
        //if true, send message to the group
        if(send_message == true) {
            try {
                msg=new Message(null, null, new Request(Request.PUT, key, value));
                channel.send(msg);
                //return prev_val;
            }
            catch(Exception e) {
                //return null;
            }
        }
        else {
            super.put(key, value);
            //don't have to do prev_val = super.put(..) as is done at the beginning
        }
        return prev_val;
    }

    /**
     * Copies all of the mappings from the specified Map to this Hashtable These mappings will replace any mappings that this Hashtable had for any of the keys currently in the specified Map.
     * @param m - Mappings to be stored in this map
     */
    @Override // GemStoneAddition
    public void putAll(Map m) {
        Message msg;
        //Changes done by <aos>
        //if true, send message to the group
        if(send_message == true) {
            try {
                msg=new Message(null, null, new Request(Request.PUT_ALL, null, m));
                channel.send(msg);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedHashtable_EXCEPTION_0, e);
            }
        }
        else {
            super.putAll(m);
        }
    }

    /**
     *  Clears this hashtable so that it contains no keys
     */
    @Override // GemStoneAddition
    public void clear() {
        Message msg;
        //Changes done by <aos>
        //if true, send message to the group
        if(send_message == true) {
            try {
                msg=new Message(null, null, new Request(Request.CLEAR, null, null));
                channel.send(msg);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedHashtable_EXCEPTION_0, e);
            }
        }
        else {
            super.clear();
        }
    }

    /**
     * Removes the key (and its corresponding value) from the Hashtable.
     * @param key - the key to be removed.
     * @return the value to which the key had been mapped in this hashtable, or null if the key did not have a mapping.
     */
    @Override // GemStoneAddition
    public Object remove(Object key) {
        Message msg;
        Object retval=null;
        retval=get(key);

        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                msg=new Message(null, null, new Request(Request.REMOVE, key, null));
                channel.send(msg);
                //return retval;
            }
            catch(Exception e) {
                //return null;
            }
        }
        else {
            super.remove(key);
            //don't have to do retval = super.remove(..) as is done at the beginning
        }
        return retval;
    }

    /*------------------------ Callbacks -----------------------*/
    Object _put(Object key, Object value) {
        Object retval=super.put(key, value);
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entrySet(key, value);
        return retval;
    }

    void _clear() {
        super.clear();
    }

    Object _remove(Object key) {
        Object retval=super.remove(key);
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entryRemoved(key);
        return retval;
    }

    /**
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void _putAll(Map m) {
        if(m == null)
            return;
        //######## The same way as in the DistributedHashtable
        // Calling the method below seems okay, but would result in ... deadlock !
        // The reason is that Map.putAll() calls put(), which we override, which results in
        // lock contention for the map.
        // ---> super.putAll(m); <--- CULPRIT !!!@#$%$
        // That said let's do it the stupid way:
        //######## The same way as in the DistributedHashtable
        Map.Entry entry;
        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            super.put(entry.getKey(), entry.getValue());
        }

        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).contentsSet(m);
    }
    /*----------------------------------------------------------*/

    /*-------------------- MessageListener ----------------------*/

    public void receive(Message msg) {
        Request req=null;

        if(msg == null)
            return;
        req=(Request)msg.getObject();
        if(req == null)
            return;
        switch(req.req_type) {
            case Request.PUT:
                if(req.key != null && req.val != null)
                    _put(req.key, req.val);
                break;
            case Request.REMOVE:
                if(req.key != null)
                    _remove(req.key);
                break;
            case Request.CLEAR:
                _clear();
                break;

            case Request.PUT_ALL:
                if(req.val != null)
                    _putAll((Map)req.val);
                break;
            default :
                // error
        }
    }

    public byte[] getState() {
        Object key, val;
        Hashtable copy=new Hashtable();

        for(Enumeration e=keys(); e.hasMoreElements();) {
            key=e.nextElement();
            val=get(key);
            copy.put(key, val);
        }
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedHashtable_EXCEPTION_MARSHALLING_STATE__0, ex);
            return null;
        }
    }

    public void setState(byte[] new_state) {
        Hashtable new_copy;
        Object key;

        try {
            new_copy=(Hashtable)Util.objectFromByteBuffer(new_state);
            if(new_copy == null) {
                notifyStateTransferCompleted(true);
                return;
            }
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedHashtable_EXCEPTION_UNMARSHALLING_STATE__0, ex);
            notifyStateTransferCompleted(false);
            return;
        }

        _clear(); // remove all elements
        for(Enumeration e=new_copy.keys(); e.hasMoreElements();) {
            key=e.nextElement();
            _put(key, new_copy.get(key));
        }
        notifyStateTransferCompleted(true);
    }

    /*-------------------- End of MessageListener ----------------------*/

    /*----------------------- MembershipListener ------------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        if(new_mbrs != null) {
            sendViewChangeNotifications(new_mbrs, members);
            // notifies observers (joined, left)
            members.removeAllElements();
            for(int i=0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
        //if size is bigger than one, there are more peers in the group
        //otherwise there is only one server.
        if(members.size() > 1) {
            send_message=true;
        }
        else {
            send_message=false;
        }
    }

    /** Called when a member is suspected */
    public void suspect(SuspectMember suspected_mbr) {
        ;
    }

    /** Block sending and receiving of messages until ViewAccepted is called */
    public void block() {
    }

    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    /*------------------- End of MembershipListener ----------------------*/

    void sendViewChangeNotifications(Vector new_mbrs, Vector old_mbrs) {
        Vector joined, left;
        Object mbr;
        Notification n;

        if(notifs.size() == 0 || old_mbrs == null || new_mbrs == null || old_mbrs.size() == 0 || new_mbrs.size() == 0)
            return;

        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        joined=new Vector();
        for(int i=0; i < new_mbrs.size(); i++) {
            mbr=new_mbrs.elementAt(i);
            if(!old_mbrs.contains(mbr))
                joined.addElement(mbr);
        }

        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        left=new Vector();
        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr)) {
                left.addElement(mbr);
            }
        }

        for(int i=0; i < notifs.size(); i++) {
            n=(Notification)notifs.elementAt(i);
            n.viewChange(joined, left);
        }
    }

    void notifyStateTransferStarted() {
        state_transfer_running=true;
        for(Iterator it=state_transfer_listeners.iterator(); it.hasNext();) {
            StateTransferListener listener=(StateTransferListener)it.next();
            try {
                listener.stateTransferStarted();
            }
            catch(Throwable t) {
            }
        }
    }

    void notifyStateTransferCompleted(boolean success) {
        state_transfer_running=false;
        for(Iterator it=state_transfer_listeners.iterator(); it.hasNext();) {
            StateTransferListener listener=(StateTransferListener)it.next();
            try {
                listener.stateTransferCompleted(success);
            }
            catch(Throwable t) {
            }
        }
    }

    private static class Request implements Serializable {
        private static final long serialVersionUID = 6677272580533789582L;
        static final int PUT=1;
        static final int REMOVE=2;
        static final int CLEAR=3;
        static final int PUT_ALL=4;

        int req_type=0;
        Object key=null;
        Object val=null;

        Request(int req_type, Object key, Object val) {
            this.req_type=req_type;
            this.key=key;
            this.val=val;
        }

        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(type2String(req_type));
            if(key != null)
                sb.append("\nkey=" + key);
            if(val != null)
                sb.append("\nval=" + val);
            return sb.toString();
        }

        String type2String(int t) {
            switch(t) {
                case PUT:
                    return "PUT";
                case REMOVE:
                    return "REMOVE";
                case CLEAR:
                    return "CLEAR";
                case PUT_ALL:
                    return "PUTALL";
                default :
                    return "<unknown>";
            }
        }

    }
}
