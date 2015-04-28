/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PERF_TP.java,v 1.11 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * Measures the time for a message to travel from the channel to the transport
 * @author Bela Ban
 * @version $Id: PERF_TP.java,v 1.11 2005/08/11 12:43:47 belaban Exp $
 */
public class PERF_TP extends Protocol  {
    private Address local_addr=null;
    static  PERF_TP instance=null;
    long    stop, start;
    long    num_msgs=0;
    long    expected_msgs=0;
    boolean done=false;


    public static PERF_TP getInstance() {
        return instance;
    }

    public PERF_TP() {
        if(instance == null)
            instance=this;
    }


    @Override // GemStoneAddition  
    public String toString() {
        return "Protocol PERF_TP (local address: " + local_addr + ')';
    }

    public boolean done() {
        return done;
    }

    public long getNumMessages() {
        return num_msgs;
    }

    public void setExpectedMessages(long m) {
        expected_msgs=m;
        num_msgs=0;
        done=false;
        start=System.currentTimeMillis();
    }

    public void reset() {
        num_msgs=expected_msgs=stop=start=0;
        done=false;
    }

    public long getTotalTime() {
        return stop-start;
    }


    /*------------------------------ Protocol interface ------------------------------ */

    @Override // GemStoneAddition  
    public String getName() {
        return "PERF_TP";
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
        Message msg;
        Address dest_addr;


        switch(evt.getType()) {

        case Event.MSG:
            if(done) {
                break;
            }
            msg=(Message)evt.getArg();
            dest_addr=msg.getDest();
            if(dest_addr == null)
                num_msgs++;
            if(num_msgs >= expected_msgs) {
                stop=System.currentTimeMillis();
                synchronized(this) {
                    done=true;
                    this.notifyAll();
                }
                if(log.isInfoEnabled()) log.info(ExternalStrings.PERF_TP_ALL_DONE_NUM_MSGS_0__EXPECTED_MSGS_1, new Object[] {Long.valueOf(num_msgs), Long.valueOf(expected_msgs)});
            }
            break;

        case Event.CONNECT:
            passUp(new Event(Event.CONNECT_OK));
            return;

        case Event.DISCONNECT:
            passUp(new Event(Event.DISCONNECT_OK));
            return;
        }

        if(down_prot != null)
            passDown(evt);
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        Address dest_addr;
        switch(evt.getType()) {

        case Event.MSG:
            if(done) {
                if(warn) log.warn("all done (discarding msg)");
                break;
            }
            msg=(Message)evt.getArg();
            dest_addr=msg.getDest();
            if(dest_addr == null)
                num_msgs++;
            if(num_msgs >= expected_msgs) {
                stop=System.currentTimeMillis();
                synchronized(this) {
                    done=true;
                    this.notifyAll();
                }
                if(log.isInfoEnabled()) log.info(ExternalStrings.PERF_TP_ALL_DONE_NUM_MSGS_0__EXPECTED_MSGS_1, new Object[] {Long.valueOf(num_msgs), Long.valueOf(expected_msgs)});
            }
            return;
        }
        passUp(evt);
    }

    /*--------------------------- End of Protocol interface -------------------------- */




}
