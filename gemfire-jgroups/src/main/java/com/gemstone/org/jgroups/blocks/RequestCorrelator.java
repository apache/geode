/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RequestCorrelator.java,v 1.24 2005/11/12 06:39:11 belaban Exp $

package com.gemstone.org.jgroups.blocks;


import java.util.concurrent.ConcurrentHashMap;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Scheduler;
import com.gemstone.org.jgroups.util.SchedulerListener;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.util.*;




/**
 * Framework to send requests and receive matching responses (matching on
 * request ID).
 * Multiple requests can be sent at a time. Whenever a response is received,
 * the correct <code>RspCollector</code> is looked up (key = id) and its
 * method <code>receiveResponse()</code> invoked. A caller may use
 * <code>done()</code> to signal that no more responses are expected, and that
 * the corresponding entry may be removed.
 * <p>
 * <code>RequestCorrelator</code> can be installed at both client and server
 * sides, it can also switch roles dynamically; i.e., send a request and at
 * the same time process an incoming request (when local delivery is enabled,
 * this is actually the default).
 * <p>
 *
 * @author Bela Ban
 */
public class RequestCorrelator  {

    /** The protocol layer to use to pass up/down messages. Can be either a Protocol or a Transport */
    protected Object transport=null;

    /** The table of pending requests (keys=Long (request IDs), values=<tt>RequestEntry</tt>) */
    protected final Map requests=new ConcurrentHashMap();

    /** The handler for the incoming requests. It is called from inside the
     * dispatcher thread */
    protected RequestHandler request_handler=null;

    /** makes the instance unique (together with IDs) */
    protected String name=null;

    /** The dispatching thread pool */
    protected Scheduler scheduler=null;


    /** The address of this group member */
    protected Address local_addr=null;

    /**
     * This field is used only if deadlock detection is enabled.
     * In case of nested synchronous requests, it holds a list of the
     * addreses of the senders with the address at the bottom being the
     * address of the first caller
     */
    protected java.util.Stack call_stack=null;

    /** Whether or not to perform deadlock detection for synchronous (potentially recursive) group method invocations.
     *  If on, we use a scheduler (handling a priority queue), otherwise we don't and call handleRequest() directly.
     */
    protected boolean deadlock_detection=false;

    /**
     * This field is used only if deadlock detection is enabled.
     * It sets the calling stack to the currently running request
     */
    protected CallStackSetter call_stack_setter=null;

    /** Process items on the queue concurrently (Scheduler). The default is to wait until the processing of an item
     * has completed before fetching the next item from the queue. Note that setting this to true
     * may destroy the properties of a protocol stack, e.g total or causal order may not be
     * guaranteed. Set this to true only if you know what you're doing ! */
    protected boolean concurrent_processing=false;


    protected boolean started=false;

    protected static final GemFireTracer log=GemFireTracer.getLog(RequestCorrelator.class);


    /**
     * Constructor. Uses transport to send messages. If <code>handler</code>
     * is not null, all incoming requests will be dispatched to it (via
     * <code>handle(Message)</code>).
     *
     * @param name Used to differentiate between different RequestCorrelators
     * (e.g. in different protocol layers). Has to be unique if multiple
     * request correlators are used.
     *
     * @param transport Used to send/pass up requests. Can be either a Transport (only send() will be
     *                  used then), or a Protocol (passUp()/passDown() will be used)
     *
     * @param handler Request handler. Method <code>handle(Message)</code>
     * will be called when a request is received.
     */
    public RequestCorrelator(String name, Object transport, RequestHandler handler) {
        this.name       = name;
        this.transport  = transport;
        request_handler = handler;
        start();
    }


    public RequestCorrelator(String name, Object transport, RequestHandler handler, Address local_addr) {
        this.name       = name;
        this.transport  = transport;
        this.local_addr=local_addr;
        request_handler = handler;
        start();
    }


    /**
     * Constructor. Uses transport to send messages. If <code>handler</code>
     * is not null, all incoming requests will be dispatched to it (via
     * <code>handle(Message)</code>).
     *
     * @param name Used to differentiate between different RequestCorrelators
     * (e.g. in different protocol layers). Has to be unique if multiple
     * request correlators are used.
     *
     * @param transport Used to send/pass up requests. Can be either a Transport (only send() will be
     *                  used then), or a Protocol (passUp()/passDown() will be used)
     *
     * @param handler Request handler. Method <code>handle(Message)</code>
     * will be called when a request is received.
     *
     * @param deadlock_detection When enabled (true) recursive synchronous
     * message calls will be detected and processed with higher priority in
     * order to solve deadlocks. Slows down processing a little bit when
     * enabled due to runtime checks involved.
     */
    public RequestCorrelator(String name, Object transport,
                             RequestHandler handler, boolean deadlock_detection) {
        this.deadlock_detection = deadlock_detection;
        this.name               = name;
        this.transport          = transport;
        request_handler         = handler;
        start();
    }


    public RequestCorrelator(String name, Object transport,
                             RequestHandler handler, boolean deadlock_detection, boolean concurrent_processing) {
        this.deadlock_detection    = deadlock_detection;
        this.name                  = name;
        this.transport             = transport;
        request_handler            = handler;
        this.concurrent_processing = concurrent_processing;
        start();
    }

    public RequestCorrelator(String name, Object transport,
                             RequestHandler handler, boolean deadlock_detection, Address local_addr) {
        this.deadlock_detection = deadlock_detection;
        this.name               = name;
        this.transport          = transport;
        this.local_addr         = local_addr;
        request_handler         = handler;
        start();
    }

    public RequestCorrelator(String name, Object transport, RequestHandler handler,
                             boolean deadlock_detection, Address local_addr, boolean concurrent_processing) {
        this.deadlock_detection    = deadlock_detection;
        this.name                  = name;
        this.transport             = transport;
        this.local_addr            = local_addr;
        request_handler            = handler;
        this.concurrent_processing = concurrent_processing;
        start();
    }




    /**
     * Switch the deadlock detection mechanism on/off
     * @param flag the deadlock detection flag
     */
    public void setDeadlockDetection(boolean flag) {
        if(deadlock_detection != flag) { // only set it if different
            deadlock_detection=flag;
            if(started) {
                if(deadlock_detection) {
                    startScheduler();
                }
                else {
                    stopScheduler();
                }
            }
        }
    }


    public void setRequestHandler(RequestHandler handler) {
        request_handler=handler;
        start();
    }


    public void setConcurrentProcessing(boolean concurrent_processing) {
        this.concurrent_processing=concurrent_processing;
    }


    /**
     * Helper method for {@link #sendRequest(long,List,Message,RspCollector)}.
     */
    public void sendRequest(long id, Message msg, RspCollector coll) {
        sendRequest(id, null, msg, coll);
    }


    /**
     * Send a request to a group. If no response collector is given, no
     * responses are expected (making the call asynchronous).
     *
     * @param id The request ID. Must be unique for this JVM (e.g. current
     * time in millisecs)
     * @param dest_mbrs The list of members who should receive the call. Usually a group RPC
     *                  is sent via multicast, but a receiver drops the request if its own address
     *                  is not in this list. Will not be used if it is null.
     * @param msg The request to be sent. The body of the message carries
     * the request data
     *
     * @param coll A response collector (usually the object that invokes
     * this method). Its methods <code>receiveResponse()</code> and
     * <code>suspect()</code> will be invoked when a message has been received
     * or a member is suspected, respectively.
     */
    public void sendRequest(long id, List dest_mbrs, Message msg, RspCollector coll) {
        Header hdr;

        if(transport == null) {
            if(log.isWarnEnabled()) log.warn("transport is not available !");
            return;
        }

        // i. Create the request correlator header and add it to the
        // msg
        // ii. If a reply is expected (sync call / 'coll != null'), add a
        // coresponding entry in the pending requests table
        // iii. If deadlock detection is enabled, set/update the call stack
        // iv. Pass the msg down to the protocol layer below
        hdr = new Header(Header.REQ, id, (coll!=null? true:false), name);
        hdr.dest_mbrs=dest_mbrs;

        if (coll != null) {
            if(deadlock_detection) {
                if(local_addr == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_LOCAL_ADDRESS_IS_NULL_);
                    return;
                }
                java.util.Stack new_call_stack = (call_stack != null?
                                                  (java.util.Stack)call_stack.clone():new java.util.Stack());
                new_call_stack.push(local_addr);
                hdr.callStack=new_call_stack;
            }
            addEntry(hdr.id, new RequestEntry(coll));
        }
        msg.putHeader(name, hdr);

        try {
            if(transport instanceof Protocol)
                ((Protocol)transport).passDown(new Event(Event.MSG, msg));
            else if(transport instanceof Transport)
                ((Transport)transport).send(msg);
            else
                if(log.isErrorEnabled())
                    log.error("transport object has to be either a " +
                              "Transport or a Protocol, however it is a " + transport.getClass());
        }
        catch(Throwable e) {
            if(log.isWarnEnabled()) log.warn(e.toString());
        }
    }





    /**
     * Used to signal that a certain request may be garbage collected as
     * all responses have been received.
     */
    public void done(long id) {
        removeEntry(id);
    }


    /**
     * <b>Callback</b>.
     * <p>
     * Called by the protocol below when a message has been received. The
     * algorithm should test whether the message is destined for us and,
     * if not, pass it up to the next layer. Otherwise, it should remove
     * the header and check whether the message is a request or response.
     * In the first case, the message will be delivered to the request
     * handler registered (calling its <code>handle()</code> method), in the
     * second case, the corresponding response collector is looked up and
     * the message delivered.
     */
    public void receive(Event evt) {
        switch(evt.getType()) {
        case Event.SUSPECT:     // don't wait for responses from faulty members
            receiveSuspect(((SuspectMember)evt.getArg()).suspectedMember); // GemStoneAddition SuspectMember struct
            break;
        case Event.VIEW_CHANGE: // adjust number of responses to wait for
            receiveView((View)evt.getArg());
            break;

        case Event.SET_LOCAL_ADDRESS:
            setLocalAddress((Address)evt.getArg());
            break;
        case Event.MSG:
            if(!receiveMessage((Message)evt.getArg()))
                return;
            break;
        }
        if(transport instanceof Protocol)
            ((Protocol)transport).passUp(evt);
        else
            if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_WE_DO_NOT_PASS_UP_MESSAGES_VIA_TRANSPORT);
    }


    /**
     */
    public void start() {
        if(deadlock_detection) {
            startScheduler();
        }
        started=true;
    }

    public void stop() {
        stopScheduler();
        started=false;
    }


    void startScheduler() {
        if(scheduler == null) {
            scheduler=new Scheduler();
            if(deadlock_detection && call_stack_setter == null) {
                call_stack_setter=new CallStackSetter();
                scheduler.setListener(call_stack_setter);
            }
            if(concurrent_processing)
                scheduler.setConcurrentProcessing(concurrent_processing);
            scheduler.start();
        }
    }


    void stopScheduler() {
        if(scheduler != null) {
            scheduler.stop();
            scheduler=null;
        }
    }


    // .......................................................................



    /**
     * <tt>Event.SUSPECT</tt> event received from a layer below.
     * <p>
     * All response collectors currently registered will
     * be notified that <code>mbr</code> may have crashed, so they won't
     * wait for its response.
     */
    public void receiveSuspect(Address mbr) {
        RequestEntry entry;
        ArrayList    copy;

        if(mbr == null) return;
        if(log.isDebugEnabled()) log.debug("suspect=" + mbr);

        // copy so we don't run into bug #761804 - Bela June 27 2003
        copy=new ArrayList(requests.values());
        for(Iterator it=copy.iterator(); it.hasNext();) {
            entry=(RequestEntry)it.next();
            if(entry.coll != null)
                entry.coll.suspect(mbr);
        }
    }


    /**
     * <tt>Event.VIEW_CHANGE</tt> event received from a layer below.
     * <p>
     * Mark all responses from members that are not in new_view as
     * NOT_RECEIVED.
     *
     */
    public void receiveView(View new_view) {
        RequestEntry entry;
        ArrayList    copy;

        // copy so we don't run into bug #761804 - Bela June 27 2003
        copy=new ArrayList(requests.values());
        for(Iterator it=copy.iterator(); it.hasNext();) {
            entry=(RequestEntry)it.next();
            if(entry.coll != null)
                entry.coll.viewChange(new_view);
        }
    }


    /**
     * Handles a message coming from a layer below
     *
     * @return true if the event should be forwarded further up, otherwise false (message was consumed)
     */
    public boolean receiveMessage(Message msg) {
        Object tmpHdr;

        // i. If header is not an instance of request correlator header, ignore
        //
        // ii. Check whether the message was sent by a request correlator with
        // the same name (there may be multiple request correlators in the same
        // protocol stack...)
        tmpHdr=msg.getHeader(name);
        if(tmpHdr == null || !(tmpHdr instanceof Header)) {
            return true;
        }

        Header hdr=(Header)tmpHdr;
        if(hdr.corrName == null || !hdr.corrName.equals(name)) {
            if(log.isTraceEnabled()) {
                log.trace(new StringBuffer("name of request correlator header (").append(hdr.corrName).
                          append(") is different from ours (").append(name).append("). Msg not accepted, passed up"));
            }
            return true;
        }

        // If the header contains a destination list, and we are not part of it, then we discard the
        // request (was addressed to other members)
        java.util.List dests=hdr.dest_mbrs;
        if(dests != null && local_addr != null && !dests.contains(local_addr)) {
            if(log.isTraceEnabled()) {
                log.trace(new StringBuffer("discarded request from ").append(msg.getSrc()).
                          append(" as we are not part of destination list (local_addr=").
                          append(local_addr).append(", hdr=").append(hdr).append(')'));
            }
            return false;
        }


        // [Header.REQ]:
        // i. If there is no request handler, discard
        // ii. Check whether priority: if synchronous and call stack contains
        // address that equals local address -> add priority request. Else
        // add normal request.
        //
        // [Header.RSP]:
        // Remove the msg request correlator header and notify the associated
        // <tt>RspCollector</tt> that a reply has been received
        switch(hdr.type) {
            case Header.REQ:
                if(request_handler == null) {
                    if(log.isWarnEnabled()) {
                        log.warn("there is no request handler installed to deliver request !");
                    }
                    return false;
                }

                if(deadlock_detection) {
                    if(scheduler == null) {
                        log.error("deadlock_detection is true, but scheduler is null: this is not supposed to happen" +
                                  " (discarding request)");
                        break;
                    }

                    Request req=new Request(msg);
                    java.util.Stack stack=hdr.callStack;
                    if(hdr.rsp_expected && stack != null && local_addr != null) {
                        if(stack.contains(local_addr)) {
                            if(log.isTraceEnabled())
                                log.trace("call stack=" + hdr.callStack + " contains " + local_addr +
                                          ": adding request to priority queue");
                            scheduler.addPrio(req);
                            break;
                        }
                    }
                    scheduler.add(req);
                    break;
                }

                handleRequest(msg);
                break;

            case Header.RSP:
                msg.removeHeader(name);
                RspCollector coll=findEntry(hdr.id);
                if(coll != null) {
                    coll.receiveResponse(msg);
                }
                break;

            default:
                msg.removeHeader(name);
                if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_HEADERS_TYPE_IS_NEITHER_REQ_NOR_RSP_);
                break;
        }

        return (false);
    }

    public Address getLocalAddress() {
        return local_addr;
    }

    public void setLocalAddress(Address local_addr) {
        this.local_addr=local_addr;
    }


    // .......................................................................

    /**
     * Add an association of:<br>
     * ID -> <tt>RspCollector</tt>
     */
    private void addEntry(long id, RequestEntry entry) {
        Long id_obj = Long.valueOf(id);
        synchronized(requests) {
            if(!requests.containsKey(id_obj))
                requests.put(id_obj, entry);
            else
                if(log.isWarnEnabled()) log.warn("entry " + entry + " for request-id=" + id + " already present !");
        }
    }


    /**
     * Remove the request entry associated with the given ID
     *
     * @param id the id of the <tt>RequestEntry</tt> to remove
     */
    private void removeEntry(long id) {
        Long id_obj = Long.valueOf(id);

        // changed by bela Feb 28 2003 (bug fix for 690606)
        // changed back to use synchronization by bela June 27 2003 (bug fix for #761804),
        // we can do this because we now copy for iteration (viewChange() and suspect())
        requests.remove(id_obj);
    }


    /**
     * @param id the ID of the corresponding <tt>RspCollector</tt>
     *
     * @return the <tt>RspCollector</tt> associated with the given ID
     */
    private RspCollector findEntry(long id) {
        Long id_obj = Long.valueOf(id);
        RequestEntry entry;

        entry=(RequestEntry)requests.get(id_obj);
        return((entry != null)? entry.coll:null);
    }


    /**
     * Handle a request msg for this correlator
     *
     * @param req the request msg
     */
    protected/*GemStoneAddition*/ void handleRequest(Message req) {
        Object        retval;
        byte[]        rsp_buf=null;
        Header        hdr, rsp_hdr;
        Message       rsp;

        // i. Remove the request correlator header from the msg and pass it to
        // the registered handler
        //
        // ii. If a reply is expected, pack the return value from the request
        // handler to a reply msg and send it back. The reply msg has the same
        // ID as the request and the name of the sender request correlator
        hdr    = (Header)req.removeHeader(name);

        if(log.isTraceEnabled()) {
            log.trace(new StringBuffer("calling (").append((request_handler != null? request_handler.getClass().getName() : "null")).
                      append(") with request ").append(hdr.id));
        }

        try {
            retval = request_handler.handle(req);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_ERROR_INVOKING_METHOD, t);
            retval=t;
        }

        if(!hdr.rsp_expected) // asynchronous call, we don't need to send a response; terminate call here
            return;

        if(transport == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_FAILURE_SENDING_RESPONSE_NO_TRANSPORT_AVAILABLE);
            return;
        }

        // changed (bela Feb 20 2004): catch exception and return exception
        try {
            rsp_buf=Util.objectToByteBuffer(retval);  // retval could be an exception, or a real value
        }
        catch(Throwable t) {
            try {
                rsp_buf=Util.objectToByteBuffer(t); // this call should succeed (all exceptions are serializable)
            }
            catch(Throwable tt) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_FAILED_SENDING_RSP_RETURN_VALUE__0__IS_NOT_SERIALIZABLE, retval);
                return;
            }
        }

        rsp=req.makeReply();
        if(rsp_buf != null)
            rsp.setBuffer(rsp_buf);
        rsp_hdr=new Header(Header.RSP, hdr.id, false, name);
        rsp.putHeader(name, rsp_hdr);
        if(log.isTraceEnabled())
            log.trace(new StringBuffer("sending rsp for ").append(rsp_hdr.id).append(" to ").append(rsp.getDest()));

        try {
            if(transport instanceof Protocol)
                ((Protocol)transport).passDown(new Event(Event.MSG, rsp));
            else if(transport instanceof Transport)
                ((Transport)transport).send(rsp);
            else
                if(log.isErrorEnabled()) log.error("transport object has to be either a " +
                                                   "Transport or a Protocol, however it is a " + transport.getClass());
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RequestCorrelator_FAILED_SENDING_THE_RESPONSE, e);
        }
    }


    // .......................................................................





    /**
     * Associates an ID with an <tt>RspCollector</tt>
     */
    private static class RequestEntry  {
        public RspCollector coll = null;

        public RequestEntry(RspCollector coll) {
            this.coll = coll;
        }
    }



    /**
     * The header for <tt>RequestCorrelator</tt> messages
     */
    public static final class Header extends com.gemstone.org.jgroups.Header implements Streamable {
        public static final byte REQ = 0;
        public static final byte RSP = 1;

        /** Type of header: request or reply */
        public byte type=REQ;
        /**
         * The id of this request to distinguish among other requests from
         * the same <tt>RequestCorrelator</tt> */
        public long id=0;

        /** msg is synchronous if true */
        public boolean rsp_expected=true;

        /** The unique name of the associated <tt>RequestCorrelator</tt> */
        public String corrName=null;

        /** Stack of Address. Contains senders (e.g. P --> Q --> R) */
        public java.util.Stack callStack=null;

        /** Contains a list of members who should receive the request (others will drop). Ignored if null */
        public java.util.List dest_mbrs=null;


        /**
         * Used for externalization
         */
        public Header() {}

        /**
         * @param type type of header (<tt>REQ</tt>/<tt>RSP</tt>)
         * @param id id of this header relative to ids of other requests
         * originating from the same correlator
         * @param rsp_expected whether it's a sync or async request
         * @param name the name of the <tt>RequestCorrelator</tt> from which
         */
        public Header(byte type, long id, boolean rsp_expected, String name) {
            this.type         = type;
            this.id           = id;
            this.rsp_expected = rsp_expected;
            this.corrName     = name;
        }

        /**
         */
        @Override // GemStoneAddition
        public String toString() {
            StringBuffer ret=new StringBuffer();
            ret.append("[Header: name=" + corrName + ", type=");
            ret.append(type == REQ ? "REQ" : type == RSP ? "RSP" : "<unknown>");
            ret.append(", id=" + id);
            ret.append(", rsp_expected=" + rsp_expected + ']');
            if(callStack != null)
                ret.append(", call stack=" + callStack);
            if(dest_mbrs != null)
                ret.append(", dest_mbrs=").append(dest_mbrs);
            return ret.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeLong(id);
            out.writeBoolean(rsp_expected);
            if(corrName != null) {
                out.writeBoolean(true);
                out.writeUTF(corrName);
            }
            else {
                out.writeBoolean(false);
            }
            out.writeObject(callStack);
            out.writeObject(dest_mbrs);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type         = in.readByte();
            id           = in.readLong();
            rsp_expected = in.readBoolean();
            if(in.readBoolean())
                corrName         = in.readUTF();
            callStack   = (java.util.Stack)in.readObject();
            dest_mbrs=(java.util.List)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(id);
            out.writeBoolean(rsp_expected);

            if(corrName != null) {
                out.writeBoolean(true);
                out.writeUTF(corrName);
            }
            else {
                out.writeBoolean(false);
            }

            if(callStack != null) {
                out.writeBoolean(true);
                out.writeShort(callStack.size());
                Address mbr;
                for(int i=0; i < callStack.size(); i++) {
                    mbr=(Address)callStack.elementAt(i);
                    Util.writeAddress(mbr, out);
                }
            }
            else {
                out.writeBoolean(false);
            }

            Util.writeAddresses(dest_mbrs, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            boolean present;
            type=in.readByte();
            id=in.readLong();
            rsp_expected=in.readBoolean();

            present=in.readBoolean();
            if(present)
                corrName=in.readUTF();

            present=in.readBoolean();
            if(present) {
                callStack=new Stack();
                short len=in.readShort();
                Address tmp;
                for(short i=0; i < len; i++) {
                    tmp=Util.readAddress(in);
                    callStack.add(tmp);
                }
            }

            dest_mbrs=(List)Util.readAddresses(in, java.util.LinkedList.class);
        }

        @Override // GemStoneAddition
        public long size(short version) {
            long retval=Global.BYTE_SIZE // type
                    + Global.LONG_SIZE // id
                    + Global.BYTE_SIZE; // rsp_expected

            retval+=Global.BYTE_SIZE; // presence for corrName
            if(corrName != null)
                retval+=corrName.length() +2; // UTF

            retval+=Global.BYTE_SIZE; // presence
            if(callStack != null) {
                retval+=Global.SHORT_SIZE; // number of elements
                if(callStack.size() > 0) {
                    Address mbr=(Address)callStack.firstElement();
                    retval+=callStack.size() * Util.size(mbr,version);
                }
            }

            retval+=Util.size(dest_mbrs, version);
            return retval;
        }

    }




    /**
     * Listens for scheduler events and sets the current call chain (stack)
     * whenever a thread is started, or a suspended thread resumed. Does
     * this only for synchronous requests (<code>Runnable</code> is actually
     * a <code>Request</code>).
     */
    protected/*GemStoneAddition*/ class CallStackSetter implements SchedulerListener {
        public void started(Runnable r)   { setCallStack(r); }
        public void stopped(Runnable r)   { setCallStack(null); }
        public void suspended(Runnable r) { setCallStack(null); }
        public void resumed(Runnable r)   { setCallStack(r); }

        void setCallStack(Runnable r) {
            java.util.Stack new_stack;
            Message req;
            Header  hdr;
            Object  obj;

            if(r == null) {
                call_stack=null;
                return;
            }

            req=((Request)r).req;
            if(req == null)
                return;

            obj=req.getHeader(name);
            if(obj == null || !(obj instanceof Header))
                return;

            hdr=(Header)obj;
            if(hdr.rsp_expected == false)
                return;

            new_stack=hdr.callStack;
            if(new_stack != null)
                call_stack=(java.util.Stack)new_stack.clone();
        }
    }


    /**
     * The runnable for an incoming request which is submitted to the
     * dispatcher
     */
    private class Request implements Runnable {
        public final Message req;

        public Request(Message req) { this.req=req; }
        public void run() { handleRequest(req); }

        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            if(req != null)
                sb.append("req=" + req + ", headers=" + req.printObjectHeaders());
            return sb.toString();
        }
    }

}
