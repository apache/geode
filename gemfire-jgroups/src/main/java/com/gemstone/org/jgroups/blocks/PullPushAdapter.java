/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PullPushAdapter.java,v 1.15 2005/10/31 10:56:31 belaban Exp $

package com.gemstone.org.jgroups.blocks;



import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Allows a client of {@link com.gemstone.org.jgroups.Channel} to be notified when messages have been received
 * instead of having to actively poll the channel for new messages. Typically used in the
 * client role (receive()). As this class does not implement interface
 * {@link com.gemstone.org.jgroups.Transport}, but <b>uses</b> it for receiving messages, an underlying object
 * has to be used to send messages (e.g. the channel on which an object of this class relies).<p>
 * Multiple MembershipListeners can register with the PullPushAdapter; when a view is received, they
 * will all be notified. There is one main message listener which sends and receives message. In addition,
 * MessageListeners can register with a certain tag (identifier), and then send messages tagged with this
 * identifier. When a message with such an identifier is received, the corresponding MessageListener will be
 * looked up and the message dispatched to it. If no tag is found (default), the main MessageListener will
 * receive the message.
 * @author Bela Ban
 * @version $Revision
 */
public class PullPushAdapter implements Runnable, ChannelListener {
    protected Transport       transport=null;
    protected MessageListener listener=null;           // main message receiver
    protected final List      membership_listeners=new ArrayList();
    protected Thread          receiver_thread=null; // Must be synchronized on instance GemStoneAddition
    protected final HashMap   listeners=new HashMap(); // keys=identifier (Serializable), values=MessageListeners
    protected final GemFireTracer log = GemFireTracer.getLog(getClass());
    static final String       PULL_HEADER="PULL_HEADER";


    public PullPushAdapter(Transport transport) {
        this.transport=transport;
        start();
    }

    public PullPushAdapter(Transport transport, MessageListener l) {
        this.transport=transport;
        setListener(l);
        start();
    }


    public PullPushAdapter(Transport transport, MembershipListener ml) {
        this.transport=transport;
        addMembershipListener(ml);
        start();
    }


    public PullPushAdapter(Transport transport, MessageListener l, MembershipListener ml) {
        this.transport=transport;
        setListener(l);
        addMembershipListener(ml);
        start();
    }


    public Transport getTransport() {
        return transport;
    }


    public void start() {
      synchronized (this) { // GemStoneAddition
        if(receiver_thread == null || !receiver_thread.isAlive()) {
            receiver_thread=new Thread(this, "PullPushAdapterThread");
            receiver_thread.setDaemon(true);
            receiver_thread.start();
        }
      }
        if(transport instanceof JChannel)
            ((JChannel)transport).addChannelListener(this);
    }

    public void stop() {
        Thread tmp;
        synchronized (this) { // GemStoneAddition
          tmp = receiver_thread;
          receiver_thread = null;
        }
        
        if(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
            try {
                tmp.join(1000);
            }
            catch(InterruptedException ex) {
              Thread.currentThread().interrupt(); // GemStoneAddition
              // propagate to caller
            }
        }
    }

    /**
     * Sends a message to the group - listeners to this identifier will receive the messages.
     * @param identifier the key that the proper listeners are listenting on 
     * @param msg the Message to be sent
     * @see #registerListener
     */
    public void send(Serializable identifier, Message msg) throws Exception {
        if(msg == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.PullPushAdapter_MSG_IS_NULL);
            return;
        }
        if(identifier == null)
            transport.send(msg);
        else {
            msg.putHeader(PULL_HEADER, new PullHeader(identifier));
            transport.send(msg);
        }
    }

    /**
     * Sends a message with no identifier; listener member will get this message on the other group members.
     * @param msg the Message to be sent
     * @throws Exception
     */
    public void send(Message msg) throws Exception {
        send(null, msg);
    }


    public void setListener(MessageListener l) {
        listener=l;
    }


    
    /**
     * Sets a listener to messages with a given identifier.
     * Messages sent with this identifier in their headers will be routed to this listener.
     * <b>Note: there can be only one listener for one identifier;
     * if you want to register a different listener to an already registered identifier, then unregister first.</b> 
     * @param identifier - messages sent on the group with this object will be received by this listener 
     * @param l - the listener that will get the message
     */
    public void registerListener(Serializable identifier, MessageListener l) {
        if(l == null || identifier == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.PullPushAdapter_MESSAGE_LISTENER_OR_IDENTIFIER_IS_NULL);
            return;
        }
        if(listeners.containsKey(identifier)) {
            if(log.isErrorEnabled()) log.error("listener with identifier=" + identifier +
                    " already exists, choose a different identifier or unregister current listener");
            // we do not want to overwrite the listener
            return;
        }
        listeners.put(identifier, l);
    }
    
    /**
     * Removes a message listener to a given identifier from the message listeners map.
     * @param identifier - the key to whom we do not want to listen any more
     */
    public void unregisterListener(Serializable identifier) {
    	listeners.remove(identifier);
    }


    /** @deprecated Use {@link #addMembershipListener} */
    @Deprecated
    public void setMembershipListener(MembershipListener ml) {
        addMembershipListener(ml);
    }

    public void addMembershipListener(MembershipListener l) {
        if(l != null && !membership_listeners.contains(l))
            membership_listeners.add(l);
    }

    public void removeMembershipListener(MembershipListener l) {
        if(l != null && membership_listeners.contains(l))
            membership_listeners.remove(l);
    }


    /**
     * Reentrant run(): message reception is serialized, then the listener is notified of the
     * message reception
     */
    public void run() {
        Object obj;

        for(;;) { // GemStoneAddition -- remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
            try {
                obj=transport.receive(0);
                if(obj == null)
                    continue;

                if(obj instanceof Message) {
                    handleMessage((Message)obj);
                }
                else if(obj instanceof GetStateEvent) {
                    byte[] retval=null;
                    if(listener != null) {
                        try {
                            retval=listener.getState();
                        }
                        catch(Throwable t) {
                            log.error(ExternalStrings.PullPushAdapter_GETSTATE_FROM_APPLICATION_FAILED_WILL_RETURN_EMPTY_STATE, t);
                        }
                    }
                    else {
                        log.warn("no listener registered, returning empty state");
                    }

                    if(transport instanceof Channel) {
                        ((Channel)transport).returnState(retval);
                    }
                    else {
                        if(log.isErrorEnabled())
                            log.error("underlying transport is not a Channel, but a " +
                                    transport.getClass().getName() + ": cannot return state using returnState()");
                        continue;
                    }
                }
                else if(obj instanceof SetStateEvent) {
                    if(listener != null) {
                        try {
                            listener.setState(((SetStateEvent)obj).getArg());
                        }
                        catch(ClassCastException cast_ex) {
                            if(log.isErrorEnabled()) log.error("received SetStateEvent, but argument " +
//                                ((SetStateEvent)obj).getArg() 
                                    (((SetStateEvent)obj).getArg() == null ? "null" : "(" + ((SetStateEvent)obj).getArg().length + " bytes)") // GemStoneAddition 
                                    + " is not serializable ! Discarding message.");
                            continue;
                        }
                    }
                }
                else if(obj instanceof View) {
                    notifyViewChange((View)obj);
                }
                else if(obj instanceof SuspectEvent) {
                    notifySuspect((SuspectEvent)obj);
                }
                else if(obj instanceof BlockEvent) {
                    notifyBlock();
                }
            }
            catch(ChannelNotConnectedException conn) {
                Address local_addr=((Channel)transport).getLocalAddress();
                if(log.isWarnEnabled()) log.warn('[' + (local_addr == null ? "<null>" : local_addr.toString()) +
                        "] channel not connected, exception is " + conn);
                try { // GemStoneAddition
                  Util.sleep(1000);
                }
                catch (InterruptedException e) {
                  // ignore, we're shutting down
                }
//                receiver_thread=null; // GemStoneAddition
                break;
            }
            catch(ChannelClosedException closed_ex) {
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                Address local_addr=((Channel)transport).getLocalAddress();
                if(log.isWarnEnabled()) log.warn('[' + (local_addr == null ? "<null>" : local_addr.toString()) +
                        "] channel closed, exception is " + closed_ex);
                // Util.sleep(1000);
//                  receiver_thread=null; // GemStoneAddition
                break;
            }
            catch (InterruptedException e) { // GemStoneAddition
              // exit loop and thread
              break;
            }
            catch(Throwable e) {
            }
        }
    }


    /**
     * Check whether the message has an identifier. If yes, lookup the MessageListener associated with the
     * given identifier in the hashtable and dispatch to it. Otherwise just use the main (default) message
     * listener
     */
    protected void handleMessage(Message msg) {
        PullHeader hdr=(PullHeader)msg.getHeader(PULL_HEADER);
        Serializable identifier;
        MessageListener l;

        if(hdr != null && (identifier=hdr.getIdentifier()) != null) {
            l=(MessageListener)listeners.get(identifier);
            if(l == null) {
                if(log.isErrorEnabled()) log.error("received a messages tagged with identifier=" +
                        identifier + ", but there is no registration for that identifier. Will drop message");
            }
            else
                l.receive(msg);
        }
        else {
            if(listener != null)
                listener.receive(msg);
        }
    }


    protected void notifyViewChange(View v) {
        MembershipListener l;

        if(v == null) return;
        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.viewAccepted(v);
            }
            catch(Throwable ex) {
              if(log.isErrorEnabled()) log.error(ExternalStrings.PullPushAdapter_EXCEPTION_NOTIFYING__0___1, new Object[] {l, ex});
            }
        }
    }

    protected void notifySuspect(SuspectEvent suspected) { // GemStoneAddition arg type
        MembershipListener l;

        if(suspected == null || suspected.getMember() == null) return;
        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.suspect(new SuspectMember((Address)suspected.getMember(), (Address)suspected.getSuspector()));
            }
            catch(Throwable ex) {
              if(log.isErrorEnabled()) log.error(ExternalStrings.PullPushAdapter_EXCEPTION_NOTIFYING__0___1, new Object[] {l, ex});
            }
        }
    }

    protected void notifyBlock() {
        MembershipListener l;

        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.block();
            }
            catch(Throwable ex) {
              if(log.isErrorEnabled()) log.error(ExternalStrings.PullPushAdapter_EXCEPTION_NOTIFYING__0___1, new Object[] {l, ex});
            }
        }
    }

    public void channelConnected(Channel channel) {
        if(log.isTraceEnabled())
            log.trace("channel is connected");
    }

    public void channelDisconnected(Channel channel) {
        if(log.isTraceEnabled())
            log.trace("channel is disconnected");
    }

    public void channelClosed(Channel channel) {
    }

    public void channelShunned() {
        if(log.isTraceEnabled())
            log.trace("channel is shunned");
    }

    public void channelReconnected(Address addr) {
        start();
    }




    public static final class PullHeader extends Header  {
        Serializable identifier=null;

        public PullHeader() {
            ; // used by externalization
        }

        public PullHeader(Serializable identifier) {
            this.identifier=identifier;
        }

        public Serializable getIdentifier() {
            return identifier;
        }

        @Override // GemStoneAddition
        public long size(short version) {
            if(identifier == null)
                return 12;
            else
                return 64;
        }


        @Override // GemStoneAddition
        public String toString() {
            return "PullHeader";
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(identifier);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            identifier=(Serializable)in.readObject();
        }
    }


	/**
	 * @return Returns the listener.
	 */
	public MessageListener getListener() {
		return listener;
	}
}
