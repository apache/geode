/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DistributedHashtable.java,v 1.20 2005/11/10 20:54:01 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.persistence.CannotPersistException;
import com.gemstone.org.jgroups.persistence.CannotRemoveException;
import com.gemstone.org.jgroups.persistence.PersistenceFactory;
import com.gemstone.org.jgroups.persistence.PersistenceManager;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.*;





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
 * regardless of one's onw location. A <code>DistributedHashtable</code> thus allows to
 * implement a distributed naming service in just a couple of lines.<p>
 * An instance of this class will contact an existing member of the group to fetch its
 * initial state (using the state exchange funclet <code>StateExchangeFunclet</code>.
 * @author Bela Ban
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 * @version $Id: DistributedHashtable.java,v 1.20 2005/11/10 20:54:01 belaban Exp $
 */
public class DistributedHashtable extends Hashtable implements MessageListener, MembershipListener {
    private static final long serialVersionUID = -1438997064696285344L;

    public interface Notification {
        void entrySet(Object key, Object value);
        void entryRemoved(Object key);
        void viewChange(Vector new_mbrs, Vector old_mbrs);
        void contentsSet(Map new_entries);
        void contentsCleared();
    }


    private transient Channel               channel;
    protected transient RpcDispatcher       disp=null;
    private transient String                groupname=null;
    private final transient Vector                notifs=new Vector();  // to be notified when mbrship changes
    private final transient Vector                members=new Vector(); // keeps track of all DHTs
    private transient Class[]               put_signature=null;
    private transient Class[]               putAll_signature=null;
    private transient Class[]               clear_signature=null;
    private transient Class[]               remove_signature=null;
    private transient boolean               persistent=false; // whether to use PersistenceManager to save state
    private transient PersistenceManager    persistence_mgr=null;

	/** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
	private transient boolean            send_message = false;

    protected final transient Promise          state_promise=new Promise();

    protected final transient GemFireTracer log=GemFireTracer.getLog(this.getClass());




    /**
     * Creates a DistributedHashtable
     * @param groupname The name of the group to join
     * @param factory The ChannelFactory which will be used to create a channel
     * @param properties The property string to be used to define the channel
     * @param state_timeout The time to wait until state is retrieved in milliseconds. A value of 0 means wait forever.
     */
    public DistributedHashtable(String groupname, ChannelFactory factory,
                                String properties, long state_timeout)
            throws ChannelException {
        this.groupname=groupname;
        initSignatures();
        channel=factory != null ? factory.createChannel(properties) : new JChannel(properties);
        disp=new RpcDispatcher(channel, this, this, this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        channel.connect(groupname);
        start(state_timeout);
    }

    /**
     * Creates a DisttributedHashtable. Optionally the contents can be saved to
     * persistemt storage using the {@link PersistenceManager}.
     * @param groupname Name of the group to join
     * @param factory Instance of a ChannelFactory to create the channel
     * @param properties Protocol stack properties
     * @param persistent Whether the contents should be persisted
     * @param state_timeout Max number of milliseconds to wait until state is
     * retrieved
     */
    public DistributedHashtable(String groupname, ChannelFactory factory, String properties,
                                boolean persistent, long state_timeout)
            throws ChannelException {
        this.groupname=groupname;
        this.persistent=persistent;
        initSignatures();
        channel=factory != null ? factory.createChannel(properties) : new JChannel(properties);
        disp=new RpcDispatcher(channel, this, this, this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        channel.connect(groupname);
        start(state_timeout);
    }


    public DistributedHashtable(JChannel channel, long state_timeout)
        throws ChannelNotConnectedException, ChannelClosedException {
        this(channel, false, state_timeout);
    }


    public DistributedHashtable(JChannel channel, boolean persistent, long state_timeout)
        throws ChannelNotConnectedException, ChannelClosedException {
        this.groupname = channel.getChannelName();
        this.channel = channel;
        this.persistent=persistent;
        init(state_timeout);
    }

    /**
     * Uses a user-provided PullPushAdapter to create the dispatcher rather than a Channel. If id is non-null, it will be
     * used to register under that id. This is typically used when another building block is already using
     * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
     * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
     * first block created on PullPushAdapter.
     * @param adapter The PullPushAdapter which to use as underlying transport
     * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
     *           requests/responses for different building blocks on top of PullPushAdapter.
     * @param state_timeout Max number of milliseconds to wait until state is
     * retrieved
     */
    public DistributedHashtable(PullPushAdapter adapter, Serializable id, long state_timeout)
        throws ChannelNotConnectedException, ChannelClosedException {
        initSignatures();
        this.channel = (Channel)adapter.getTransport();
        this.groupname = this.channel.getChannelName();
        disp=new RpcDispatcher(adapter, id, this, this, this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        start(state_timeout);
    }

    public DistributedHashtable(PullPushAdapter adapter, Serializable id) {
        initSignatures();
        this.channel = (Channel)adapter.getTransport();
        this.groupname = this.channel.getChannelName();
        disp=new RpcDispatcher(adapter, id, this, this, this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
    }

    protected void init(long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        initSignatures();
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        disp = new RpcDispatcher(channel, this, this, this);

        // Changed by bela (jan 20 2003): start() has to be called by user (only when providing
        // own channel). First, Channel.connect() has to be called, then start().
        // start(state_timeout);
    }


    /**
     * Fetches the state
     * @param state_timeout
     * @throws ChannelClosedException
     * @throws ChannelNotConnectedException
     */
    public void start(long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        boolean rc;
        if(persistent) {
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_FETCHING_STATE_FROM_DATABASE);
            try {
                persistence_mgr=PersistenceFactory.getInstance().createManager();
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled()) log.error("failed creating PersistenceManager, " +
                            "turning persistency off. Exception: " + Util.printStackTrace(ex));
                persistent=false;
            }
        }

        state_promise.reset();
        rc=channel.getState(null, state_timeout);
        if(rc) {
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_STATE_WAS_RETRIEVED_SUCCESSFULLY_WAITING_FOR_SETSTATE);
            Boolean result=(Boolean)state_promise.getResult(state_timeout);
            if(result == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_SETSTATE_NEVER_GOT_CALLED);
            }
            else {
                if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_SETSTATE_WAS_CALLED);
            }
        }
        else {
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_STATE_COULD_NOT_BE_RETRIEVED_FIRST_MEMBER);
            if(persistent) {
                if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_FETCHING_STATE_FROM_DATABASE);
                try {
                    Map m=persistence_mgr.retrieveAll();
                    if(m != null) {
                        Map.Entry entry;
                        Object key, val;
                        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
                            entry=(Map.Entry)it.next();
                            key=entry.getKey();
                            val=entry.getValue();
                            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedHashtable_INSERTING__0____1, new Object[] {key, val});
                            put(key, val);  // will replicate key and value
                        }
                    }
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled()) log.error("failed creating PersistenceManager, " +
                                "turning persistency off. Exception: " + Util.printStackTrace(ex));
                    persistent=false;
                }
            }
        }
    }


    public Address getLocalAddress()        {return channel != null ? channel.getLocalAddress() : null;}
    public String  getGroupName()           {return groupname;}
    public Channel getChannel()             {return channel;}
    public boolean getPersistent()          {return persistent;}
    public void    setPersistent(boolean p) {persistent=p;}

    public void addNotifier(Notification n) {
        if(!notifs.contains(n))
            notifs.addElement(n);
    }

    public void removeNotifier(Notification n) {
        if(notifs.contains(n))
            notifs.removeElement(n);
    }

    public void stop() {
        if(disp != null) {
            disp.stop();
            disp=null;
        }
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


	/**
	 * Maps the specified key to the specified value in the hashtable. Neither of both parameters can be null
	 * @param key - the hashtable key
	 * @param value - the value
	 * @return the previous value of the specified key in this hashtable, or null if it did not have one
	 */
    @Override // GemStoneAddition
    public Object put(Object key, Object value) {
        Object prev_val=get(key);

        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                disp.callRemoteMethods(
                        null, "_put", new Object[]{key,value},
                        put_signature,
                        GroupRequest.GET_ALL,
                        0);
            }
            catch(Exception e) {
                //return null;
            }
        }
        else {
            _put(key, value);
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
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            try {
                disp.callRemoteMethods(
                        null, "_putAll", new Object[]{m}, 
                        putAll_signature,
                        GroupRequest.GET_ALL,
                        0);
            }
            catch(Throwable t) {
            }
        }
        else {
            _putAll(m);
        }
    }

	/**
	 * Clears this hashtable so that it contains no keys
	 */
    @Override // GemStoneAddition
	public synchronized void clear() {
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            try {
                disp.callRemoteMethods(
                        null, "_clear", null,
                        clear_signature,
                        GroupRequest.GET_ALL,
                        0);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_EXCEPTION_0, e);
            }
        }
        else {
            _clear();
        }
    }

	/**
	 * Removes the key (and its corresponding value) from the Hashtable.
	 * @param key - the key to be removed.
	 * @return the value to which the key had been mapped in this hashtable, or null if the key did not have a mapping.
	 */
    @Override // GemStoneAddition
	public Object remove(Object key) {
		Object retval = get(key);

		//Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                disp.callRemoteMethods(
                        null, "_remove", new Object[]{key},
                        remove_signature,
                        GroupRequest.GET_ALL,
                        0);
                //return retval;
            }
            catch(Exception e) {
                //return null;
            }
        }
        else {
            _remove(key);
            //don't have to do retval = super.remove(..) as is done at the beginning
        }
        return retval;
    }



    /*------------------------ Callbacks -----------------------*/

    public Object _put(Object key, Object value) {
        Object retval=super.put(key, value);
        if(persistent) {
            try {
                persistence_mgr.save((Serializable)key, (Serializable)value);
            }
            catch(CannotPersistException cannot_persist_ex) {
                if(log.isErrorEnabled()) log.error("failed persisting " + key + " + " +
                            value + ", exception=" + cannot_persist_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed persisting " + key + " + " +
                            value + ", exception=" + Util.printStackTrace(t));
            }
        }
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entrySet(key, value);
        return retval;
    }


    /**
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void _putAll(Map m) {
        if (m == null)
            return;

        // Calling the method below seems okay, but would result in ... deadlock !
        // The reason is that Map.putAll() calls put(), which we override, which results in
        // lock contention for the map.

        // ---> super.putAll(m); <--- CULPRIT !!!@#$%$

        // That said let's do it the stupid way:
        Map.Entry entry;
        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            super.put(entry.getKey(), entry.getValue());
        }

        if (persistent) {
            try {
                persistence_mgr.saveAll(m);
            }
            catch (CannotPersistException persist_ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_PERSISTING_CONTENTS__0, persist_ex);
            }
            catch (Throwable t) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_PERSISTING_CONTENTS__0, t);
            }
        }
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).contentsSet(m);
    }


    public void _clear() {
        super.clear();
        if(persistent) {
            try {
                persistence_mgr.clear();
            }
            catch(CannotRemoveException cannot_remove_ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_CLEARING_CONTENTS_EXCEPTION_0, cannot_remove_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_CLEARING_CONTENTS_EXCEPTION_0, t);
            }
        }
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).contentsCleared();
    }


    public Object _remove(Object key) {
        Object retval=super.remove(key);
        if(persistent) {
            try {
                persistence_mgr.remove((Serializable)key);
            }
            catch(CannotRemoveException cannot_remove_ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_CLEARING_CONTENTS_EXCEPTION_0, cannot_remove_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_FAILED_CLEARING_CONTENTS_EXCEPTION_0, t);
            }
        }
        for(int i=0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entryRemoved(key);

        return retval;
    }

    /*----------------------------------------------------------*/



    /*-------------------- State Exchange ----------------------*/

    public void receive(Message msg) { }

    public byte[] getState() {
        Object    key, val;
        Hashtable copy=new Hashtable();

        for(Enumeration e=keys(); e.hasMoreElements();) {
            key=e.nextElement();
            val=get(key);
            copy.put(key, val);
        }
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_EXCEPTION_MARSHALLING_STATE__0, ex);
            return null;
        }
    }


    public void setState(byte[] new_state) {
        Hashtable new_copy;

        try {
            new_copy=(Hashtable)Util.objectFromByteBuffer(new_state);
            if(new_copy == null)
                return;
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_EXCEPTION_UNMARSHALLING_STATE__0, ex);
            return;
        }
        _putAll(new_copy);
        state_promise.setResult(Boolean.TRUE);
    }



    /*------------------- Membership Changes ----------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        if(new_mbrs != null) {
            sendViewChangeNotifications(new_mbrs, members); // notifies observers (joined, left)
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
    public void block() {}


    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    void sendViewChangeNotifications(Vector new_mbrs, Vector old_mbrs) {
        Vector        joined, left;
        Object        mbr;
        Notification  n;

        if(notifs.size() == 0 || old_mbrs == null || new_mbrs == null ||
           old_mbrs.size() == 0 || new_mbrs.size() == 0)
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


    void initSignatures() {
        try {
            if(put_signature == null) {
                put_signature=new Class[] {Object.class,Object.class};
            }

            if(putAll_signature == null) {
                putAll_signature=new Class[] {Map.class};
            }

            if(clear_signature == null)
                clear_signature=new Class[0];

            if(remove_signature == null) {
                remove_signature=new Class[] {Object.class};
            }
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedHashtable_EXCEPTION_0, ex);
        }
    }

//    public static void main(String[] args) {
//        try {
//            // The setup here is kind of weird:
//            // 1. Create a channel
//            // 2. Create a DistributedHashtable (on the channel)
//            // 3. Connect the channel (so the HT gets a VIEW_CHANGE)
//            // 4. Start the HT
//            //
//            // A simpler setup is
//            // DistributedHashtable ht = new DistributedHashtable("demo", null,
//            //         "file://c:/JGroups-2.0/conf/state_transfer.xml", 5000);
//
//            JChannel c = new JChannel("file:/c:/JGroups-2.0/conf/state_transfer.xml");
//            c.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
//            DistributedHashtable ht = new DistributedHashtable(c, false, 5000);
//            c.connect("demo");
//            ht.start(5000);
//
//
//
//            ht.put("name", "Michelle Ban");
//            Object old_key = ht.remove("name");
//            System.out.println("old key was " + old_key);
//            ht.put("newkey", "newvalue");
//
//            Map m = new HashMap();
//            m.put("k1", "v1");
//            m.put("k2", "v2");
//
//            ht.putAll(m);
//
//            System.out.println("hashmap is " + ht);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch (Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//        }
//    }

}

