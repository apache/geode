/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: NotificationBus.java,v 1.9 2005/07/17 11:36:40 chrislott Exp $

package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.Vector;


/**
 * This class provides notification sending and handling capability. 
 * Producers can send notifications to all registered consumers.
 * Provides hooks to implement shared group state, which allows an
 * application programmer to maintain a local cache which is replicated 
 * by all instances. NotificationBus sits on 
 * top of a channel, however it creates its channel itself, so the 
 * application programmers do not have to provide their own channel. 
 *
 * @author Bela Ban
 */
public class NotificationBus implements MessageListener, MembershipListener {
    final Vector members=new Vector();
    JChannel channel=null;
    Address local_addr=null;
    PullPushAdapter ad=null;
    Consumer consumer=null; // only a single consumer allowed
    String bus_name="notification_bus";
    final Promise get_cache_promise=new Promise();
    final Object cache_mutex=new Object();

    protected final GemFireTracer log=GemFireTracer.getLog(getClass());


    String props=null;


    public interface Consumer {
        void handleNotification(Serializable n);

        /** Called on the coordinator to obtains its cache */
        Serializable getCache();

        void memberJoined(Address mbr);

        void memberLeft(Address mbr);
    }


    public NotificationBus() throws Exception {
        this(null, null);
    }


    public NotificationBus(String bus_name) throws Exception {
        this(bus_name, null);
    }


    public NotificationBus(String bus_name, String properties) throws Exception {
        if(bus_name != null) this.bus_name=bus_name;
        if(properties != null) props=properties;
        channel=new JChannel(props);
    }


    public void setConsumer(Consumer c) {
        consumer=c;
    }


    public Address getLocalAddress() {
        if(local_addr != null) return local_addr;
        if(channel != null)
            local_addr=channel.getLocalAddress();
        return local_addr;
    }


    /**
     * Returns a reference to the real membership: don't modify. 
     * If you need to modify, make a copy first !
     * @return Vector of Address objects
     */
    public Vector getMembership() {
        return members;
    }


    /** 
     * Answers the Channel.
     * Used to operate on the underlying channel directly, e.g. perform operations that are not
     * provided using only NotificationBus. Should be used sparingly.
     * @return underlying Channel
     */
    public Channel getChannel() {
        return channel;
    }


    public boolean isCoordinator() {
        Object first_mbr=null;

        synchronized(members) {
            first_mbr=members.size() > 0 ? members.elementAt(0) : null;
            if(first_mbr == null)
                return true;
        }
        if(getLocalAddress() != null)
            return getLocalAddress().equals(first_mbr);
        return false;
    }


    public void start() throws Exception {
        channel.connect(bus_name);
        ad=new PullPushAdapter(channel, this, this);
    }


    public void stop() {
        if(ad != null) {
            ad.stop();
            ad=null;
        }
        if(channel != null) {
            channel.close();  // disconnects from channel and closes it
            channel=null;
        }
    }


    /** Pack the argument in a Info, serialize that one into the message buffer and send the message */
    public void sendNotification(Serializable n) {
        Message msg=null;
        byte[] data=null;
        Info info;

        try {
            if(n == null) return;
            info=new Info(Info.NOTIFICATION, n);
            data=Util.objectToByteBuffer(info);
            msg=new Message(null, null, data);
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null. " +
                                                                  " Won't send notification");
                return;
            }
            channel.send(msg);
        }
        catch(Throwable ex) {

                if(log.isErrorEnabled()) log.error(ExternalStrings.NotificationBus_EXCEPTION_IS__0, ex);
        }
    }


    /**
     Determines the coordinator and asks it for its cache. If there is no coordinator (because we are first member),
     null will be returned. Used only internally by NotificationBus.
     @param timeout Max number of msecs until the call returns
     @param max_tries Max number of attempts to fetch the cache from the coordinator
     */
    public Serializable getCacheFromCoordinator(long timeout, int max_tries) {
        return getCacheFromMember(null, timeout, max_tries);
    }


    /**
     Determines the coordinator and asks it for its cache. If there is no coordinator (because we are first member),
     null will be returned. Used only internally by NotificationBus.
     @param mbr The address of the member from which to fetch the state. If null, the current coordinator
     will be asked for the state
     @param timeout Max number of msecs until the call returns - if timeout elapses
     null will be returned
     @param max_tries Max number of attempts to fetch the cache from the coordinator (will be set to 1 if < 1)
     */
    public Serializable getCacheFromMember(Address mbr, long timeout, int max_tries) {
        Serializable cache=null;
        int num_tries=0;
        Info info; // GemStoneAddition =new Info(Info.GET_CACHE_REQ);
        Message msg;
        Address dst=mbr;  // member from which to fetch the cache

        long start, stop; // +++ remove


        if(max_tries < 1) max_tries=1;

        get_cache_promise.reset();
        while(num_tries <= max_tries) {
            if(mbr == null) {  // mbr == null means get cache from coordinator
                dst=determineCoordinator();
                if(dst == null || dst.equals(getLocalAddress())) { // we are the first member --> empty cache
                    if(log.isInfoEnabled()) log.info("[" + getLocalAddress() +
                                                     "] no coordinator found --> first member (cache is empty)");
                    return null;
                }
            }

            // +++ remove
            if(log.isInfoEnabled()) log.info("[" + getLocalAddress() + "] dst=" + dst +
                                             ", timeout=" + timeout + ", max_tries=" + max_tries + ", num_tries=" + num_tries);

            info=new Info(Info.GET_CACHE_REQ);
            msg=new Message(dst, null, info);
            channel.down(new Event(Event.MSG, msg));

            start=System.currentTimeMillis();
            cache=(Serializable) get_cache_promise.getResult(timeout);
            stop=System.currentTimeMillis();
            if(cache != null) {
                if(log.isInfoEnabled()) log.info("got cache from " +
                                                 dst + ": cache is valid (waited " + (stop - start) + " msecs on get_cache_promise)");
                return cache;
            }
            else {
                if(log.isErrorEnabled()) log.error("received null cache; retrying (waited " +
                                                   (stop - start) + " msecs on get_cache_promise)");
            }

            try { // GemStoneAddition
              Util.sleep(500);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              log.error(ExternalStrings.NotificationBus_THREAD_INTERRUPTED);
              // treat as timeout
              break;
            }
            ++num_tries;
        }
//        if(cache == null) GemStoneAddition (can only be null)
            if(log.isErrorEnabled()) log.error("[" + getLocalAddress() +
                                               "] cache is null (num_tries=" + num_tries + ')');
        return cache;
    }


    /**
     Don't multicast this to all members, just apply it to local consumers.
     */
    public void notifyConsumer(Serializable n) {
        if(consumer != null && n != null)
            consumer.handleNotification(n);
    }


    /* -------------------------------- Interface MessageListener -------------------------------- */
    public void receive(Message msg) {
        Info info=null;
        Object obj;

        if(msg == null || msg.getLength() == 0) return;
        try {
            obj=msg.getObject();
            if(!(obj instanceof Info)) {

                    if(log.isErrorEnabled()) log.error("expected an instance of Info (received " +
                                                             obj.getClass().getName() + ')');
                return;
            }
            info=(Info) obj;
            switch(info.type) {
                case Info.NOTIFICATION:
                    notifyConsumer(info.data);
                    break;

                case Info.GET_CACHE_REQ:
                    handleCacheRequest(msg.getSrc());
                    break;

                case Info.GET_CACHE_RSP:
                    // +++ remove
                    if(log.isDebugEnabled()) log.debug("[GET_CACHE_RSP] cache was received from " + msg.getSrc());
                    get_cache_promise.setResult(info.data);
                    break;

                default:
                    if(log.isErrorEnabled()) log.error(ExternalStrings.NotificationBus_TYPE__0__UNKNOWN, info.type);
                    break;
            }
        }
        catch(Throwable ex) {

                if(log.isErrorEnabled()) log.error(ExternalStrings.NotificationBus_EXCEPTION_0, ex);
        }
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    /* ----------------------------- End of Interface MessageListener ---------------------------- */




    /* ------------------------------- Interface MembershipListener ------------------------------ */

    public synchronized void viewAccepted(View new_view) {
        Vector joined_mbrs, left_mbrs, tmp;
        Object tmp_mbr;

        if(new_view == null) return;
        tmp=new_view.getMembers();

        synchronized(members) {
            // get new members
            joined_mbrs=new Vector();
            for(int i=0; i < tmp.size(); i++) {
                tmp_mbr=tmp.elementAt(i);
                if(!members.contains(tmp_mbr))
                    joined_mbrs.addElement(tmp_mbr);
            }

            // get members that left
            left_mbrs=new Vector();
            for(int i=0; i < members.size(); i++) {
                tmp_mbr=members.elementAt(i);
                if(!tmp.contains(tmp_mbr))
                    left_mbrs.addElement(tmp_mbr);
            }

            // adjust our own membership
            members.removeAllElements();
            members.addAll(tmp);
        }

        if(consumer != null) {
            if(joined_mbrs.size() > 0)
                for(int i=0; i < joined_mbrs.size(); i++)
                    consumer.memberJoined((Address) joined_mbrs.elementAt(i));
            if(left_mbrs.size() > 0)
                for(int i=0; i < left_mbrs.size(); i++)
                    consumer.memberLeft((Address) left_mbrs.elementAt(i));
        }
    }


    public void suspect(SuspectMember suspected_mbr) {
    }

    public void block() {
    }


    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    /* ----------------------------- End of Interface MembershipListener ------------------------- */







    /* ------------------------------------- Private Methods ------------------------------------- */

    Address determineCoordinator() {
        Vector v=channel != null ? channel.getView().getMembers() : null;
        return v != null ? (Address) v.elementAt(0) : null;
    }


    void handleCacheRequest(Address sender) {
        Serializable cache=null;
        Message msg;
        Info info;

        if(sender == null) {
            // +++ remove
            //
            if(log.isErrorEnabled()) log.error(ExternalStrings.NotificationBus_SENDER_IS_NULL);
            return;
        }

        synchronized(cache_mutex) {
            cache=getCache(); // get the cache from the consumer
            info=new Info(Info.GET_CACHE_RSP, cache);
            msg=new Message(sender, null, info);
            if(log.isInfoEnabled()) log.info(ExternalStrings.NotificationBus__0__RETURNING_CACHE_TO__1, new Object[] {getLocalAddress(), sender});
            channel.down(new Event(Event.MSG, msg));
        }
    }

    public Serializable getCache() {
        return consumer != null ? consumer.getCache() : null;
    }



    /* --------------------------------- End of Private Methods ---------------------------------- */





    private static class Info implements Serializable {
        private static final long serialVersionUID = -2247826108262348005L;
        public final static int NOTIFICATION=1;
        public final static int GET_CACHE_REQ=2;
        public final static int GET_CACHE_RSP=3;


        int type=0;
        Serializable data=null;  // if type == NOTIFICATION data is notification, if type == GET_CACHE_RSP, data is cache


        public Info(int type) {
            this.type=type;
        }

        public Info(int type, Serializable data) {
            this.type=type;
            this.data=data;
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("type= ");
            if(type == NOTIFICATION)
                sb.append("NOTIFICATION");
            else if(type == GET_CACHE_REQ)
                sb.append("GET_CACHE_REQ");
            else if(type == GET_CACHE_RSP)
                sb.append("GET_CACHE_RSP");
            else
                sb.append("<unknown>");
            if(data != null) {
                if(type == NOTIFICATION)
                    sb.append(", notification=" + data);
                else if(type == GET_CACHE_RSP) sb.append(", cache=" + data);
            }
            return sb.toString();
        }
    }


}



