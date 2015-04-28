/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DUMMY_TP.java,v 1.1 2005/04/14 14:39:32 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.stack.Protocol;


/**
 * Dummy transport, returns a fake local address and responds to CONNECT with CONNECT_OK.
 * Compared to LOOPBACK, this discards everything
 * @author Bela Ban
 * @version $Id: DUMMY_TP.java,v 1.1 2005/04/14 14:39:32 belaban Exp $
 */
public class DUMMY_TP extends Protocol {
    private Address local_addr=null;

    public DUMMY_TP() {
    }


    @Override // GemStoneAddition
    public String toString() {
        return "Protocol DUMMY_TP (local address: " + local_addr + ')';
    }


    /*------------------------------ Protocol interface ------------------------------ */

    @Override // GemStoneAddition
    public String getName() {
        return "DUMMY_TP";
    }




    @Override // GemStoneAddition
    public void init() throws Exception {
        local_addr=new com.gemstone.org.jgroups.stack.IpAddress("localhost", 10000); // fake address
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

        switch(evt.getType()) {

        case Event.CONNECT:
            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            passUp(new Event(Event.DISCONNECT_OK));
            break;
        }
    }



    /*--------------------------- End of Protocol interface -------------------------- */




}
