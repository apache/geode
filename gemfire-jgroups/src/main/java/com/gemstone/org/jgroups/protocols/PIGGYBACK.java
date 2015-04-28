/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PIGGYBACK.java,v 1.10 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Vector;


/**
 * Combines multiple messages into a single large one. As many messages as possible are combined into
 * one, after a max timeout or when the msg size becomes too big the message is sent. On the receiving
 * side, the large message is spliced into the smaller ones and delivered.
 */

public class PIGGYBACK extends Protocol  {
    long max_wait_time=20; // milliseconds: max. wait between consecutive msgs
    long max_size=8192;    // don't piggyback if created msg would exceed this size (in bytes)
    final Queue msg_queue=new Queue();
    Packer packer=null;
    boolean packing=false;
    Address local_addr=null;


    class Packer implements Runnable {
        Thread t=null; // GemStoneAddition -- accesses synchronized on this


        synchronized /* GemStoneAddition */ public void start() {
            if(t == null) {
                t=new Thread(this, "Packer thread");
                t.setDaemon(true);
                t.start();
            }
        }

        synchronized /* GemStoneAddition */ public void stop() {
          if (t != null) t.interrupt(); // GemStoneAddition
            t=null;
        }

        public void run() {
            long current_size=0;
            long start_time, time_to_wait=max_wait_time;
            Message m, new_msg;
            Vector msgs;

            for (;;) { // GemStoneAddition -- avoid anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    m=(Message)msg_queue.remove();
                    m.setSrc(local_addr);
                    start_time=System.currentTimeMillis();
                    current_size=0;
                    new_msg=new Message();
                    msgs=new Vector();
                    msgs.addElement(m);
                    current_size+=m.size();

                    while(System.currentTimeMillis() - start_time <= max_wait_time &&
                            current_size <= max_size) {

                        time_to_wait=max_wait_time - (System.currentTimeMillis() - start_time);
                        if(time_to_wait <= 0)
                            break;

                        try {
                            m=(Message)msg_queue.peek(time_to_wait);
                            m.setSrc(local_addr);
                        }
                        catch(TimeoutException timeout) {
                            break;
                        }
                        if(/*m == null || GemStoneAddition not possible */ m.size() + current_size > max_size)
                            break;
                        m=(Message)msg_queue.remove();
                        current_size+=m.size();
                        msgs.addElement(m);
                    }

                    try {
                        new_msg.putHeader(getName(), new PiggybackHeader());
                        new_msg.setBuffer(Util.objectToByteBuffer(msgs));
                        passDown(new Event(Event.MSG, new_msg));

                            if(log.isInfoEnabled()) log.info("combined " + msgs.size() +
                                    " messages of a total size of " + current_size + " bytes");
                    }
                    catch(Exception e) {
                        if(warn) log.warn("exception is " + e);
                    }
                }
                catch (InterruptedException ie) { // GemStoneAddition
                     if(log.isInfoEnabled()) log.info(ExternalStrings.PIGGYBACK_PACKER_STOPPED_AS_QUEUE_IS_INTERRUPTED);
                    break; // No need to reset interrupt bit; just exit.
                }                    
                catch(QueueClosedException closed) {
                     if(log.isInfoEnabled()) log.info(ExternalStrings.PIGGYBACK_PACKER_STOPPED_AS_QUEUE_IS_CLOSED);
                    break;
                }
            }
        }
    }


    /**
     * All protocol names have to be unique !
     */
    @Override // GemStoneAddition  
    public String getName() {
        return "PIGGYBACK";
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {super.setProperties(props);
        String str;

        str=props.getProperty("max_wait_time");
        if(str != null) {
            max_wait_time=Long.parseLong(str);
            props.remove("max_wait_time");
        }
        str=props.getProperty("max_size");
        if(str != null) {
            max_size=Long.parseLong(str);
            props.remove("max_size");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.PIGGYBACK_PIGGYBACKSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public void start() throws Exception {
        startPacker();
    }

    @Override // GemStoneAddition  
    public void stop() {
        packing=false;
        msg_queue.close(true);  // flush pending messages, this should also stop the packer ...
        stopPacker();           // ... but for safety reasons, we stop it here again
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        Object obj;
        Vector messages;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.MSG:
                msg=(Message)evt.getArg();
                obj=msg.getHeader(getName());
                if(obj == null || !(obj instanceof PiggybackHeader))
                    break;

                msg.removeHeader(getName());
                try {
                    messages=(Vector)msg.getObject();
                     if(log.isInfoEnabled()) log.info(ExternalStrings.PIGGYBACK_UNPACKING__0__MESSAGES, messages.size());
                    for(int i=0; i < messages.size(); i++)
                        passUp(new Event(Event.MSG, messages.elementAt(i)));
                }
                catch(Exception e) {
                    if(warn) log.warn("piggyback message does not contain a vector of " +
                            "piggybacked messages, discarding message ! Exception is " + e);
                    return;
                }

                return;             // don't pass up !
        }

        passUp(evt);            // Pass up to the layer above us
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message msg;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();

                if(msg.getDest() != null && !msg.getDest().isMulticastAddress())
                    break;  // unicast message, handle as usual

                if(!packing)
                    break;  // pass down as usual; we haven't started yet

                try {
                    msg_queue.add(msg);
                }
                catch(QueueClosedException closed) {
                    break;  // pass down regularly
                }
                return;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    void startPacker() {
        if(packer == null) {
            packing=true;
            packer=new Packer();
            packer.start();
        }
    }


    void stopPacker() {
        if(packer != null) {
            packer.stop();
            packing=false;
            msg_queue.close(false);
            packer=null;
        }
    }


    public static class PiggybackHeader extends Header  {

        public PiggybackHeader() {
        }

        @Override // GemStoneAddition  
        public String toString() {
            return "[PIGGYBACK: <variables> ]";
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }

    }


}
