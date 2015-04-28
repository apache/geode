/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: LOOPBACK.java,v 1.16 2005/08/26 12:26:33 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.Util;


/**
 Makes copies of outgoing messages, swaps sender and receiver and sends the message back up the stack.
 */
public class LOOPBACK extends Protocol {
    private Address local_addr=null;
    private String group_addr=null;

    public LOOPBACK() {
    }


    @Override // GemStoneAddition  
    public String toString() {
        return "Protocol LOOPBACK(local address: " + local_addr + ')';
    }




    /*------------------------------ Protocol interface ------------------------------ */

    @Override // GemStoneAddition  
    public String getName() {
        return "LOOPBACK";
    }



    @Override // GemStoneAddition  
    public void init() throws Exception {
//        local_addr=new IpAddress("localhost", 10000) { // fake address
//            public String toString() {
//                return "<fake>";
//            }
//        };

          //local_addr=new org.jgroups.stack.IpAddress("localhost", 10000); // fake address
       local_addr = new IpAddress(12345);
    }

    @Override // GemStoneAddition  
    public void start() throws Exception {
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling Down).
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
        if(trace)
            log.trace("event is " + evt + ", group_addr=" + group_addr +
                      ", time is " + System.currentTimeMillis() + ", hdrs: " + Util.printEvent(evt));

        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Message rsp=msg.copy();
            if(rsp.getSrc() == null)
                rsp.setSrc(local_addr);

            //dest_addr=msg.getDest();
            //rsp.setDest(local_addr);
            //rsp.setSrc(dest_addr != null ? dest_addr : local_addr);
            up(new Event(Event.MSG, rsp));
            break;

        case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            break;

        case Event.CONNECT:
            group_addr=(String)evt.getArg();
            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            passUp(new Event(Event.DISCONNECT_OK));
            break;

        case Event.PERF:
            passUp(evt);
            break;
        }
    }



    /*--------------------------- End of Protocol interface -------------------------- */


}
