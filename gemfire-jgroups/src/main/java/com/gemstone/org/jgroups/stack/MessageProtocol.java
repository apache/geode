/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: MessageProtocol.java,v 1.5 2005/11/12 06:37:41 belaban Exp $

package com.gemstone.org.jgroups.stack;



import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.blocks.RequestCorrelator;
import com.gemstone.org.jgroups.blocks.RequestHandler;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Rsp;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.util.Vector;




/**
 * Based on Protocol, but incorporates RequestCorrelator and GroupRequest: the latter can
 * be used to mcast messages to all members and receive their reponses.<p>
 * A protocol based on this template can send messages to all members and receive all, a single,
 * n, or none responses. Requests directed towards the protocol can be handled by overriding
 * method <code>Handle</code>.<p>
 * Requests and responses are in the form of <code>Message</code>s, which would typically need to
 * contain information pertaining to the request/response, e.g. in the form of objects contained
 * in the message. To use remote method calls, use <code>RpcProtocol</code> instead.<p>
 * Typical use of of a <code>MessageProtocol</code> would be when a protocol needs to interact with
 * its peer protocols at each of the members' protocol stacks. A simple protocol like fragmentation,
 * which does not need to interact with other instances of fragmentation, may simply subclass
 * <code>Protocol</code> instead.
 * @author Bela Ban
 */
public abstract class MessageProtocol extends Protocol implements RequestHandler {
    protected RequestCorrelator _corr=null;
    protected final Vector members=new Vector();


    @Override // GemStoneAddition  
    public void start() throws Exception {
        if(_corr == null)
            _corr=new RequestCorrelator(getName(), this, this);
        _corr.start();
    }

    @Override // GemStoneAddition  
    public void stop() {
        if(_corr != null) {
            _corr.stop();
            // _corr=null;
        }
    }


    /**
     Cast a message to all members, and wait for <code>mode</code> responses. The responses are
     returned in a response list, where each response is associated with its sender.<p>
     Uses <code>GroupRequest</code>.
     @param dests The members from which responses are expected. If it is null, replies from all members
     are expected. The request itself is multicast to all members.
     @param msg The message to be sent to n members
     @param mode Defined in <code>GroupRequest</code>. The number of responses to wait for:
     <ol>
     <li>GET_FIRST: return the first response received.
     <li>GET_ALL: wait for all responses (minus the ones from suspected members)
     <li>GET_MAJORITY: wait for a majority of all responses (relative to the grp size)
     <li>GET_ABS_MAJORITY: wait for majority (absolute, computed once)
     <li>GET_N: wait for n responses (may block if n > group size)
     <li>GET_NONE: wait for no responses, return immediately (non-blocking)
     </ol>
     @param timeout If 0: wait forever. Otherwise, wait for <code>mode</code> responses
     <em>or</em> timeout time.
     @return RspList A list of responses. Each response is an <code>Object</code> and associated
     to its sender.
     */
    public RspList castMessage(Vector dests, Message msg, int mode, long timeout) {
        GroupRequest _req=null;
        Vector real_dests=dests != null? (Vector)dests.clone() : (Vector)members.clone();

        // This marks message as sent by us ! (used in up()
        // msg.addHeader(new MsgProtHeader(getName()));   ++ already done by RequestCorrelator

        _req=new GroupRequest(msg, _corr, real_dests, mode, timeout, 0);
        _req.execute();

        return _req.getResults();
    }


    /**
     Sends a message to a single member (destination = msg.dest) and returns the response.
     The message's destination must be non-zero !
     */
    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        Vector mbrs=new Vector();
        RspList rsp_list=null;
        Object dest=msg.getDest();
        Rsp rsp;
        GroupRequest _req=null;

        if(dest == null) {
            System.out.println("MessageProtocol.sendMessage(): the message's destination is null ! " +
                               "Cannot send message !");
            return null;
        }


        mbrs.addElement(dest);   // dummy membership (of destination address)


        _req=new GroupRequest(msg, _corr, mbrs, mode, timeout, 0);
        _req.execute();

        if(mode == GroupRequest.GET_NONE)
            return null;


        rsp_list=_req.getResults();

        if(rsp_list.size() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.MessageProtocol_RESPONSE_LIST_IS_EMPTY);
            return null;
        }
        if(rsp_list.size() > 1)
            if(log.isErrorEnabled()) log.error("response list contains " +
                                                         "more that 1 response; returning first response");
        rsp=(Rsp)rsp_list.elementAt(0);
        if(rsp.wasSuspected())
            throw new SuspectedException(dest);
        if(!rsp.wasReceived())
            throw new TimeoutException();
        return rsp.getValue();
    }


    /**
     Processes a request destined for this layer. The return value is sent as response.
     */
    public Object handle(Message req) {
        System.out.println("MessageProtocol.handle(): this method should be overridden !");
        return null;
    }


    /**
     * Handle an event coming from the layer above
     */
    @Override // GemStoneAddition  
    public final void up(Event evt) {
        Message msg;
        Object hdr;

        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                updateView((View)evt.getArg());
                break;
            default:
                if(!handleUpEvent(evt)) return;

                if(evt.getType() == Event.MSG) {
                    msg=(Message)evt.getArg();
                    hdr=msg.getHeader(getName());
                    if(!(hdr instanceof RequestCorrelator.Header))
                        break;
                }
                // [[[ TODO
                // RequestCorrelator.receive() is currently calling passUp()
                // itself. Only _this_ method should call passUp()!
                // So return instead of breaking until fixed (igeorg)
                // ]]] TODO
                if(_corr != null) {
                    _corr.receive(evt);
                    return;
                }
                else
                    if(log.isWarnEnabled()) log.warn("Request correlator is null, evt=" + Util.printEvent(evt));

                break;
        }

        passUp(evt);
    }


    /**
     * This message is not originated by this layer, therefore we can just
     * pass it down without having to go through the request correlator.
     * We do this ONLY for messages !
     */
    @Override // GemStoneAddition  
    public final void down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                updateView((View)evt.getArg());
                if(!handleDownEvent(evt)) return;
                break;
            case Event.MSG:
                if(!handleDownEvent(evt)) return;
                break;
            default:
                if(!handleDownEvent(evt)) return;
                break;
        }

        passDown(evt);
    }


    protected void updateView(View new_view) {
        Vector new_mbrs=new_view.getMembers();
        if(new_mbrs != null) {
            synchronized(members) {
                members.removeAllElements();
                members.addAll(new_mbrs);
            }
        }
    }


    /**
     Handle up event. Return false if it should not be passed up the stack.
     */
    protected boolean handleUpEvent(Event evt) {
        // override in subclasses
        return true;
    }

    /**
     Handle down event. Return false if it should not be passed down the stack.
     */
    protected boolean handleDownEvent(Event evt) {
        // override in subclasses
        return true;
    }


}
