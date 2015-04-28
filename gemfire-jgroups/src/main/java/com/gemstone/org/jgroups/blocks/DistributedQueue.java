/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DistributedQueue.java,v 1.16 2005/07/17 11:36:40 chrislott Exp $
package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.*;


/**
 * Provides the abstraction of a java.util.LinkedList that is replicated at several
 * locations. Any change to the list (reset, add, remove, etc.) will transparently be
 * propagated to all replicas in the group. All read-only methods will always access the
 * local replica.<p>
 * Both keys and values added to the list <em>must be serializable</em>, the reason
 * being that they will be sent across the network to all replicas of the group.
 * An instance of this class will contact an existing member of the group to fetch its
 * initial state.
 * Beware to use a <em>total protocol</em> on initialization or elements would not be in same
 * order on all replicas.
 * @author Romuald du Song
 */
public class DistributedQueue implements MessageListener, MembershipListener, Cloneable
{
    public interface Notification
    {
        void entryAdd(Object value);

        void entryRemoved(Object key);

        void viewChange(Vector new_mbrs, Vector old_mbrs);

        void contentsCleared();

        void contentsSet(Collection new_entries);
    }

    protected GemFireTracer logger = GemFireTracer.getLog(getClass());
    private long internal_timeout = 10000; // 10 seconds to wait for a response

    /*lock object for synchronization*/
    protected Object mutex = new Object();
    protected transient boolean stopped = false; // whether to we are stopped !
    protected LinkedList internalQueue;
    protected transient Channel channel;
    protected transient RpcDispatcher disp = null;
    protected transient String groupname = null;
    protected transient Vector notifs = new Vector(); // to be notified when mbrship changes
    protected transient Vector members = new Vector(); // keeps track of all DHTs
    private transient Class[] add_signature = null;
    private transient Class[] addAtHead_signature = null;
    private transient Class[] addAll_signature = null;
    private transient Class[] reset_signature = null;
    private transient Class[] remove_signature = null;
    
    /**
     * Creates a DistributedQueue
     * @param groupname The name of the group to join
     * @param factory The ChannelFactory which will be used to create a channel
     * @param properties The property string to be used to define the channel
     * @param state_timeout The time to wait until state is retrieved in milliseconds. A value of 0 means wait forever.
     */
    public DistributedQueue(String groupname, ChannelFactory factory, String properties, long state_timeout)
                     throws ChannelException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("DistributedQueue(" + groupname + ',' + properties + ',' + state_timeout);
        }

        this.groupname = groupname;
        initSignatures();
        internalQueue = new LinkedList();
        channel = (factory != null) ? factory.createChannel(properties) : new JChannel(properties);
        disp = new RpcDispatcher(channel, this, this, this);
        disp.setDeadlockDetection(false); // To ensure strict FIFO MethodCall
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        channel.connect(groupname);
        start(state_timeout);
    }

    public DistributedQueue(JChannel channel)
    {
        this.groupname = channel.getChannelName();
        this.channel = channel;
        init();
    }

    /**
      * Uses a user-provided PullPushAdapter to create the dispatcher rather than a Channel. If id is non-null, it will be
      * used to register under that id. This is typically used when another building block is already using
      * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
      * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
      * first block created on PullPushAdapter.
      * The caller needs to call start(), before using the this block. It gives the opportunity for the caller
      * to register as a lessoner for Notifications events.
      * @param adapter The PullPushAdapter which to use as underlying transport
      * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
      *           requests/responses for different building blocks on top of PullPushAdapter.
      */
    public DistributedQueue(PullPushAdapter adapter, Serializable id)
    {
        this.channel = (Channel)adapter.getTransport();
        this.groupname = this.channel.getChannelName();

        initSignatures();
        internalQueue = new LinkedList();

        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        disp = new RpcDispatcher(adapter, id, this, this, this);
        disp.setDeadlockDetection(false); // To ensure strict FIFO MethodCall
    }

    protected void init()
    {
        initSignatures();
        internalQueue = new LinkedList();
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        disp = new RpcDispatcher(channel, this, this, this);
        disp.setDeadlockDetection(false); // To ensure strict FIFO MethodCall
    }

    public void start(long state_timeout) throws ChannelClosedException, ChannelNotConnectedException
    {
        boolean rc;
        logger.debug("DistributedQueue.initState(" + groupname + "): starting state retrieval");

        rc = channel.getState(null, state_timeout);

        if (rc)
        {
            logger.info(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUEINITSTATE_0__STATE_WAS_RETRIEVED_SUCCESSFULLY, groupname);
        }
        else
        {
            logger.info(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUEINITSTATE_0__STATE_COULD_NOT_BE_RETRIEVED_FIRST_MEMBER, groupname);
        }
    }

    public Address getLocalAddress()
    {
        return (channel != null) ? channel.getLocalAddress() : null;
    }

    public Channel getChannel()
    {
        return channel;
    }

    public void addNotifier(Notification n)
    {
        if (n != null && !notifs.contains(n))
        {
            notifs.addElement(n);
        }
    }

    public void removeNotifier(Notification n)
    {
        notifs.removeElement(n);
    }

    public void stop()
    {
        /*lock the queue from other threads*/
        synchronized (mutex)
        {
            internalQueue.clear();

            if (disp != null)
            {
                disp.stop();
                disp = null;
            }

            if (channel != null)
            {
                channel.close();
                channel = null;
            }

            stopped = true;
        }
    }

    /**
     * Add the speficied element at the bottom of the queue
     * @param value
     */
    public void add(Object value)
    {
        try
        {
            Object retval = null;

            RspList rsp = disp.callRemoteMethods(null, "_add", new Object[]{value}, add_signature, GroupRequest.GET_ALL, 0);
            Vector results = rsp.getResults();

            if (results.size() > 0)
            {
                retval = results.elementAt(0);

                if (logger.isDebugEnabled())
                {
                    checkResult(rsp, retval);
                }
            }
        }
         catch (Exception e)
        {
            logger.error(ExternalStrings.DistributedQueue_UNABLE_TO_ADD_VALUE__0, value, e);
        }

        return;
    }

    /**
     * Add the specified element at the top of the queue
     * @param value
     */
    public void addAtHead(Object value)
    {
        try
        {
            disp.callRemoteMethods(null, "_addAtHead", new Object[]{value}, addAtHead_signature, GroupRequest.GET_ALL, 0);
        }
         catch (Exception e)
        {
            logger.error(ExternalStrings.DistributedQueue_UNABLE_TO_ADDATHEAD_VALUE__0, value, e);
        }

        return;
    }

    /**
     * Add the speficied collection to the top of the queue.
     * Elements are added in the order that they are returned by the specified
     * collection's iterator.
     * @param values
     */
    public void addAll(Collection values)
    {
        try
        {
            disp.callRemoteMethods(null, "_addAll", new Object[]{values}, addAll_signature, GroupRequest.GET_ALL, 0);
        }
         catch (Exception e)
        {
            logger.error(ExternalStrings.DistributedQueue_UNABLE_TO_ADDALL_VALUE__0, values, e);
        }

        return;
    }

    public Vector getContents()
    {
        Vector result = new Vector();

        for (Iterator e = internalQueue.iterator(); e.hasNext();)
            result.add(e.next());

        return result;
    }

    public int size()
    {
        return internalQueue.size();
    }

    /**
      * returns the first object on the queue, without removing it.
      * If the queue is empty this object blocks until the first queue object has
      * been added
      * @return the first object on the queue
      */
    public Object peek()
    {
        Object retval = null;

        try
        {
            retval = internalQueue.getFirst();
        }
         catch (NoSuchElementException e)
        {
        }

        return retval;
    }

    public void reset()
    {
        try
        {
            disp.callRemoteMethods(null, "_reset", null, reset_signature, GroupRequest.GET_ALL, 0);
        }
         catch (Exception e)
        {
            logger.error(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUERESET_0, groupname, e);
        }
    }

    protected void checkResult(RspList rsp, Object retval)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Value updated from " + groupname + " :" + retval);
        }

        Vector results = rsp.getResults();

        for (int i = 0; i < results.size(); i++)
        {
            Object data = results.elementAt(i);

            if (!data.equals(retval))
            {
                logger.error(ExternalStrings.DistributedQueue_REFERENCE_VALUE_DIFFERS_FROM_RETURNED_VALUE__0____1, new Object[] {retval, data});
            }
        }
    }

    /**
     * Try to return the first objet in the queue.It does not wait for an object.
     * @return the first object in the queue or null if none were found.
     */
    public Object remove()
    {
        Object retval = null;
        RspList rsp = disp.callRemoteMethods(null, "_remove", null, remove_signature, GroupRequest.GET_ALL, internal_timeout);
        Vector results = rsp.getResults();

        if (results.size() > 0)
        {
            retval = results.elementAt(0);

            if (logger.isDebugEnabled())
            {
                checkResult(rsp, retval);
            }
        }

        return retval;
    }

    /**
     * @param timeout The time to wait until an entry is retrieved in milliseconds. A value of 0 means wait forever.
     * @return the first object in the queue or null if none were found
     */
    public Object remove(long timeout)
    {
        Object retval = null;
        long start = System.currentTimeMillis();

        if (timeout <= 0)
        {
            while (!stopped && (retval == null))
            {
                RspList rsp = disp.callRemoteMethods(null, "_remove", null, remove_signature, GroupRequest.GET_ALL, internal_timeout);
                Vector results = rsp.getResults();

                if (results.size() > 0)
                {
                    retval = results.elementAt(0);

                    if (logger.isDebugEnabled())
                    {
                        checkResult(rsp, retval);
                    }
                }

                if (retval == null)
                {
                    try
                    {
                        synchronized (mutex)
                        {
                            mutex.wait();
                        }
                    }
                     catch (InterruptedException e)
                    {
                       Thread.currentThread().interrupt(); // GemStoneAddition
                       return null; // GemStoneAddition - treat as failure
                    }
                }
            }
        }
        else
        {
            while (((System.currentTimeMillis() - start) < timeout) && !stopped && (retval == null))
            {
                RspList rsp = disp.callRemoteMethods(null, "_remove", null, remove_signature, GroupRequest.GET_ALL, internal_timeout);
                Vector results = rsp.getResults();

                if (results.size() > 0)
                {
                    retval = results.elementAt(0);

                    if (logger.isDebugEnabled())
                    {
                        checkResult(rsp, retval);
                    }
                }

                if (retval == null)
                {
                    try
                    {
                        long delay = timeout - (System.currentTimeMillis() - start);

                        synchronized (mutex)
                        {
                            if (delay > 0)
                            {
                                mutex.wait(delay);
                            }
                        }
                    }
                     catch (InterruptedException e)
                    {
                       Thread.currentThread().interrupt(); // GemStoneAddition
                       break; // GemStoneAddition - treat as timeout
                    }
                }
            }
        }

        return retval;
    }

    @Override // GemStoneAddition
    public String toString()
    {
        return internalQueue.toString();
    }

    /*------------------------ Callbacks -----------------------*/
    public void _add(Object value)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(groupname + '@' + getLocalAddress() + " _add(" + value + ')');
        }

        /*lock the queue from other threads*/
        synchronized (mutex)
        {
            internalQueue.add(value);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }

        for (int i = 0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entryAdd(value);
    }

    public void _addAtHead(Object value)
    {
        /*lock the queue from other threads*/
        synchronized (mutex)
        {
            internalQueue.addFirst(value);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }

        for (int i = 0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).entryAdd(value);
    }

    public void _reset()
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(groupname + '@' + getLocalAddress() + " _reset()");
        }

        _private_reset();

        for (int i = 0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).contentsCleared();
    }

    protected void _private_reset()
    {
        /*lock the queue from other threads*/
        synchronized (mutex)
        {
            internalQueue.clear();

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }

    public Object _remove()
    {
        Object retval = null;

        try
        {
            /*lock the queue from other threads*/
            synchronized (mutex)
            {
                retval = internalQueue.removeFirst();

                /*wake up all the threads that are waiting for the lock to be released*/
                mutex.notifyAll();
            }

            if (logger.isDebugEnabled())
            {
                logger.debug(groupname + '@' + getLocalAddress() + "_remove(" + retval + ')');
            }

            for (int i = 0; i < notifs.size(); i++)
                ((Notification)notifs.elementAt(i)).entryRemoved(retval);
        }
         catch (NoSuchElementException e)
        {
            logger.debug(groupname + '@' + getLocalAddress() + "_remove(): nothing to remove");
        }

        return retval;
    }

    public void _addAll(Collection c)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(groupname + '@' + getLocalAddress() + " _addAll(" + c + ')');
        }

        /*lock the queue from other threads*/
        synchronized (mutex)
        {
            internalQueue.addAll(c);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }

        for (int i = 0; i < notifs.size(); i++)
            ((Notification)notifs.elementAt(i)).contentsSet(c);
    }

    /*----------------------------------------------------------*/
    /*-------------------- State Exchange ----------------------*/
    public void receive(Message msg)
    {
    }

    public byte[] getState()
    {
        Vector copy = (Vector)getContents().clone();

        try
        {
            return Util.objectToByteBuffer(copy);
        }
         catch (Throwable ex)
        {
            logger.error(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUEGETSTATE_EXCEPTION_MARSHALLING_STATE, ex);

            return null;
        }
    }

    public void setState(byte[] new_state)
    {
        Vector new_copy;

        try
        {
            new_copy = (Vector)Util.objectFromByteBuffer(new_state);

            if (new_copy == null)
            {
                return;
            }
        }
         catch (Throwable ex)
        {
            logger.error(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUESETSTATE_EXCEPTION_UNMARSHALLING_STATE, ex);

            return;
        }

        _private_reset(); // remove all elements      
        _addAll(new_copy);
    }

    /*------------------- Membership Changes ----------------------*/
    public void viewAccepted(View new_view)
    {
        Vector new_mbrs = new_view.getMembers();

        if (new_mbrs != null)
        {
            sendViewChangeNotifications(new_mbrs, members); // notifies observers (joined, left)
            members.removeAllElements();

            for (int i = 0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
    }

    /** Called when a member is suspected */
    public void suspect(SuspectMember suspected_mbr)
    {
        ;
    }

    /** Block sending and receiving of messages until ViewAccepted is called */
    public void block()
    {
    }

    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    void sendViewChangeNotifications(Vector new_mbrs, Vector old_mbrs)
    {
        Vector joined;
        Vector left;
        Object mbr;
        Notification n;

        if ((notifs.size() == 0) || (old_mbrs == null) || (new_mbrs == null) || (old_mbrs.size() == 0) ||
                (new_mbrs.size() == 0))
        {
            return;
        }

        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        joined = new Vector();

        for (int i = 0; i < new_mbrs.size(); i++)
        {
            mbr = new_mbrs.elementAt(i);

            if (!old_mbrs.contains(mbr))
            {
                joined.addElement(mbr);
            }
        }

        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        left = new Vector();

        for (int i = 0; i < old_mbrs.size(); i++)
        {
            mbr = old_mbrs.elementAt(i);

            if (!new_mbrs.contains(mbr))
            {
                left.addElement(mbr);
            }
        }

        for (int i = 0; i < notifs.size(); i++)
        {
            n = (Notification)notifs.elementAt(i);
            n.viewChange(joined, left);
        }
    }

    void initSignatures()
    {
        try
        {
            if (add_signature == null)
            {
                add_signature = new Class[] { Object.class };
            }

            if (addAtHead_signature == null)
            {
                addAtHead_signature = new Class[] { Object.class };
            }

            if (addAll_signature == null)
            {
                addAll_signature = new Class[] { Collection.class };
            }

            if (reset_signature == null)
            {
                reset_signature = new Class[0];
            }

            if (remove_signature == null)
            {
                remove_signature = new Class[0];
            }
        }
         catch (Throwable ex)
        {
           logger.error(ExternalStrings.DistributedQueue_DISTRIBUTEDQUEUEINITMETHODS, ex);
        }
    }

//    public static void main(String[] args)
//    {
//        try
//        {
//            // The setup here is kind of weird:
//            // 1. Create a channel
//            // 2. Create a DistributedQueue (on the channel)
//            // 3. Connect the channel (so the HT gets a VIEW_CHANGE)
//            // 4. Start the HT
//            //
//            // A simpler setup is
//            // DistributedQueue ht = new DistributedQueue("demo", null, 
//            //         "file://c:/JGroups-2.0/conf/total-token.xml", 5000);
//            JChannel c = new JChannel("file:/c:/JGroups-2.0/conf/conf/total-token.xml");
//            c.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
//
//            DistributedQueue ht = new DistributedQueue(c);
//            c.connect("demo");
//            ht.start(5000);
//
//            ht.add("name");
//            ht.add("Michelle Ban");
//
//            Object old_key = ht.remove();
//            System.out.println("old key was " + old_key);
//            old_key = ht.remove();
//            System.out.println("old value was " + old_key);
//
//            ht.add("name 'Michelle Ban'");
//
//            System.out.println("queue is " + ht);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//         catch (Throwable t)
//        {
//           SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//        }
//    }
}
