/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RpcDispatcher.java,v 1.20 2005/11/12 06:39:21 belaban Exp $

package com.gemstone.org.jgroups.blocks;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Channel;
import com.gemstone.org.jgroups.ChannelListener;
import com.gemstone.org.jgroups.MembershipListener;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.MessageListener;
import com.gemstone.org.jgroups.SuspectedException;
import com.gemstone.org.jgroups.TimeoutException;
import com.gemstone.org.jgroups.Transport;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.reflect.Method;




/**
 * This class allows a programmer to invoke remote methods in all (or single) 
 * group members and optionally wait for the return value(s). 
 * An application will typically create a channel and layer the
 * RpcDispatcher building block on top of it, which allows it to 
 * dispatch remote methods (client role) and at the same time be 
 * called by other members (server role).
 * This class is derived from MessageDispatcher. 
*  Is the equivalent of RpcProtocol on the application rather than protocol level.
 * @author Bela Ban
 */
public class RpcDispatcher extends MessageDispatcher implements ChannelListener {
    protected Object        server_obj=null;
    protected Marshaller    marshaller=null;
    protected List          additionalChannelListeners=null;
    protected MethodLookup  method_lookup=null;


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
        additionalChannelListeners = new ArrayList();
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection) {
        super(channel, l, l2, deadlock_detection);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
        additionalChannelListeners = new ArrayList();
    }

    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection, boolean concurrent_processing) {
        super(channel, l, l2, deadlock_detection, concurrent_processing);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
        additionalChannelListeners = new ArrayList();
    }



    public RpcDispatcher(PullPushAdapter adapter, Serializable id,
                         MessageListener l, MembershipListener l2, Object server_obj) {
        super(adapter, id, l, l2);

        // Fixes bug #804956
        // channel.setChannelListener(this);
        if(this.adapter != null) {
            Transport t=this.adapter.getTransport();
            if(t != null && t instanceof Channel) {
                ((Channel)t).addChannelListener(this);
            }
        }

        this.server_obj=server_obj;
        additionalChannelListeners = new ArrayList();
    }


    public interface Marshaller {
        byte[] objectToByteBuffer(Object obj) throws Exception;
        Object objectFromByteBuffer(byte[] buf) throws Exception;
    }


    public String getName() {return "RpcDispatcher";}

    public void       setMarshaller(Marshaller m) {this.marshaller=m;}

    public Marshaller getMarshaller()             {return marshaller;}

    public Object getServerObject() {return server_obj;}

    public MethodLookup getMethodLookup() {
        return method_lookup;
    }

    public void setMethodLookup(MethodLookup method_lookup) {
        this.method_lookup=method_lookup;
    }


    @Override // GemStoneAddition
    public RspList castMessage(Vector dests, Message msg, int mode, long timeout) {
        if(log.isErrorEnabled()) log.error("this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }

    @Override // GemStoneAddition
    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        if(log.isErrorEnabled()) log.error("this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }





    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout) {
        if(dests != null && dests.size() == 0) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuffer("destination list of ").append(method_call.getName()).
                          append("() is empty: no need to send message"));
            return new RspList();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuffer("dests=").append(dests).append(", method_call=").append(method_call).
                      append(", mode=").append(mode).append(", timeout=").append(timeout));

        byte[] buf;
        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_EXCEPTION_0, e);
            return null;
        }

        Message msg=new Message(null, null, buf);
        RspList  retval=super.castMessage(dests, msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }



    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   String[] signature, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        byte[]   buf=null;
        Message  msg=null;
        Object   retval=null;

        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_EXCEPTION_0, e);
            return null;
        }

        msg=new Message(dest, null, buf);
        retval=super.sendMessage(msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        return retval;
    }





    /**
     * Message contains MethodCall. Execute it against *this* object and return result.
     * Use MethodCall.invoke() to do this. Return result.
     */
    @Override // GemStoneAddition
    public Object handle(Message req) {
        Object      body=null;
        MethodCall  method_call;

        if(server_obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_NO_METHOD_HANDLER_IS_REGISTERED_DISCARDING_REQUEST);
            return null;
        }

        if(req == null || req.getLength() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_MESSAGE_OR_MESSAGE_BUFFER_IS_NULL);
            return null;
        }

        try {
            body=marshaller != null? marshaller.objectFromByteBuffer(req.getBuffer()) : req.getObject();
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_EXCEPTION_0, e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcDispatcher_MESSAGE_DOES_NOT_CONTAIN_A_METHODCALL_OBJECT);
            return null;
        }

        method_call=(MethodCall)body;

        try {
            if(log.isTraceEnabled())
                log.trace("[sender=" + req.getSrc() + "], method_call: " + method_call);

            if(method_call.getMode() == MethodCall.ID) {
                if(method_lookup == null)
                    throw new Exception("MethodCall uses ID=" + method_call.getId() + ", but method_lookup has not been set");
                Method m=method_lookup.findMethod(method_call.getId());
                if(m == null)
                    throw new Exception("no method foudn for " + method_call.getId());
                method_call.setMethod(m);
            }
            
            return method_call.invoke(server_obj);
        }
        catch(Throwable x) {
            log.error(ExternalStrings.RpcDispatcher_FAILED_INVOKING_METHOD, x);
            return x;
        }
    }

    /**
     * Add a new channel listener to be notified on the channel's state change.
     *
     * @return true if the listener was added or false if the listener was already in the list.
     */
    public boolean addChannelListener(ChannelListener l) {

        synchronized(additionalChannelListeners) {
            if (additionalChannelListeners.contains(l)) {
               return false;
            }
            additionalChannelListeners.add(l);
            return true;
        }
    }


    /**
     *
     * @return true if the channel was removed indeed.
     */
    public boolean removeChannelListener(ChannelListener l) {

        synchronized(additionalChannelListeners) {
            return additionalChannelListeners.remove(l);
        }
    }



    /* --------------------- Interface ChannelListener ---------------------- */

    public void channelConnected(Channel channel) {

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelConnected(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelDisconnected(Channel channel) {

        stop();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelDisconnected(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelClosed(Channel channel) {

        stop();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelClosed(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelShunned() {

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelShunned();
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelReconnected(Address new_addr) {
        if(log.isTraceEnabled())
            log.trace("channel has been rejoined, old local_addr=" + local_addr + ", new local_addr=" + new_addr);
        this.local_addr=new_addr;
        start();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelReconnected(new_addr);
                }
                catch(Throwable t) {
                   log.warn("channel listener failed", t);
                }
            }
        }
    }
    /* ----------------------------------------------------------------------- */

}
