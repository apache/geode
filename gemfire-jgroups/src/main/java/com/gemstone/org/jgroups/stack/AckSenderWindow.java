/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: AckSenderWindow.java,v 1.17 2005/08/26 08:56:22 belaban Exp $

package com.gemstone.org.jgroups.stack;


import java.util.concurrent.ConcurrentHashMap;
import com.gemstone.org.jgroups.protocols.TP;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;


/**
 * ACK-based sliding window for a sender. Messages are added to the window keyed by seqno
 * When an ACK is received, the corresponding message is removed. The Retransmitter
 * continously iterates over the entries in the hashmap, retransmitting messages based on their
 * creation time and an (increasing) timeout. When there are no more messages in the retransmission
 * table left, the thread terminates. It will be re-activated when a new entry is added to the
 * retransmission table.
 * @author Bela Ban
 */
public class AckSenderWindow implements Retransmitter.RetransmitCommand {
    RetransmitCommand   retransmit_command = null;   // called to request XMIT of msg
    final Map     msgs=new ConcurrentHashMap();        // keys: seqnos (Long), values: Messages
    long[]              interval = new long[]{400,800,1200,1600};
    final Retransmitter retransmitter;
    static    final GemFireTracer log=GemFireTracer.getLog(AckSenderWindow.class);
    Address dest = null; // GemStoneAddition - added for debugging so we know dest for xmit tasks
    
    public interface RetransmitCommand {
        void retransmit(long seqno, Message msg);
        // GemStoneAddition - retransmission burst containment (bug #40467)
        long getMaxRetransmissionBurst();
    }


    /**
     * Creates a new instance. Thre retransmission thread has to be started separately with
     * <code>start()</code>.
     * @param com If not null, its method <code>retransmit()</code> will be called when a message
     *            needs to be retransmitted (called by the Retransmitter).
     */
    public AckSenderWindow(RetransmitCommand com) {
        retransmit_command = com;
        retransmitter = new Retransmitter(null, this);
        retransmitter.setRetransmitTimeouts(interval);
    }


    public AckSenderWindow(RetransmitCommand com, long[] interval) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new Retransmitter(null, this);
        retransmitter.setRetransmitTimeouts(interval);
    }



    public AckSenderWindow(RetransmitCommand com, long[] interval, TimeScheduler sched) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new Retransmitter(null, this, sched);
        retransmitter.setRetransmitTimeouts(interval);
    }



    public void reset() {
      synchronized (msgs) { // GemStoneAddition
        msgs.clear();
      }

        // moved out of sync scope: Retransmitter.reset()/add()/remove() are sync'ed anyway
        // Bela Jan 15 2003
        retransmitter.reset();
    }


    /**
     * Adds a new message to the retransmission table. If the message won't have received an ack within
     * a certain time frame, the retransmission thread will retransmit the message to the receiver. If
     * a sliding window protocol is used, we only add up to <code>window_size</code> messages. If the table is
     * full, we add all new messages to a queue. Those will only be added once the table drains below a certain
     * threshold (<code>min_threshold</code>)
     */
    public void add(long seqno, Message msg) {
        Long tmp=Long.valueOf(seqno);
        synchronized(msgs) {  // the contains() and put() should be atomic
          if (this.dest == null) {
            this.dest = msg.getDest();
          }
          if(!msgs.containsKey(tmp))
            msgs.put(tmp, msg);
          retransmitter.add(seqno,seqno);
        }
    }


    /**
     * Removes the message from <code>msgs</code>, removing them also from retransmission. If
     * sliding window protocol is used, and was queueing, check whether we can resume adding elements.
     * Add all elements. If this goes above window_size, stop adding and back to queueing. Else
     * set queueing to false.
     * @return the size of the message if the message was still in the window and required an ack, or -1 if not found
     */
    public long ack(long seqno) {
      final Message msg;
        synchronized(msgs) {
          msg = (Message)msgs.remove(Long.valueOf(seqno));
          if (msg == null) {
            return -1;
          }
          retransmitter.remove(seqno);
        }
        return msg.size();
    }

    public int size() {
      synchronized (msgs) { // GemStoneAddition
        return msgs.size();
      }
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        //sb.append(msgs.size()).append(" msgs (").append(retransmitter.size()).append(" to retransmit): ");
        sb.append("retransmitter: ");
//        retransmitter.toString(sb);
        TreeSet keys=new TreeSet(msgs.keySet());
        if(keys.size() > 0)
            sb.append('{').append(keys.first()).append(" - ").append(keys.last()).append('}');
        else
            sb.append("{}");
        return sb.toString();
    }


    public String printDetails() {
        StringBuffer sb=new StringBuffer();
        sb.append(msgs.size()).append(" msgs (").append(retransmitter.size()).append(" to retransmit): ").
                append(new TreeSet(msgs.keySet()));
        return sb.toString();
    }

    /* -------------------------------- Retransmitter.RetransmitCommand interface ------------------- */
    public Address getDest() {
      return this.dest;
    }
    
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        Message msg;
        Address dest = null;
        long burstLimit = retransmit_command.getMaxRetransmissionBurst(); // GemStoneAddition - max burst size
        long burstSize = 0;

        if (retransmit_command != null) {
            for (long i = first_seqno; i <= last_seqno; i++) {
              // GemStoneAddition: fetch msg under synchronization
              synchronized (msgs) {
                msg = (Message) msgs.get(Long.valueOf(i));                
              }
                if(msg != null) { // find the message to retransmit
                    retransmit_command.retransmit(i, msg);
                    dest = msg.getDest();
                    if (burstLimit > 0) {
                      burstSize += msg.size();
                      if (burstSize >= burstLimit) {
                        break;
                      }
                    }
                }
            }
            if (log.isTraceEnabled() || TP.VERBOSE) {
              StringBuffer b = new StringBuffer("retransmitted message");
              if (last_seqno == first_seqno) {
                b.append(' ').append(first_seqno);
              }
              else {
                b.append("s ").append(first_seqno).
                          append(" - ").append(last_seqno);
              }
              if (dest != null) {
                b.append(" to ").append(dest);
              }
              log.getLogWriter().info(ExternalStrings.DEBUG, b);
            }
        }
    }
    /* ----------------------------- End of Retransmitter.RetransmitCommand interface ---------------- */





    /* ---------------------------------- Private methods --------------------------------------- */

    /* ------------------------------ End of Private methods ------------------------------------ */




    /** Struct used to store message alongside with its seqno in the message queue */
    static/*GemStoneAddition*/ class Entry {
        final long seqno;
        final Message msg;

        Entry(long seqno, Message msg) {
            this.seqno = seqno;
            this.msg = msg;
        }
    }


    static class Dummy implements RetransmitCommand {
        static/*GemStoneAddition*/ final long last_xmit_req = 0;
//         long curr_time; GemStoneAddition (omitted)


        public void retransmit(long seqno, Message msg) {

                if(log.isDebugEnabled()) log.debug("seqno=" + seqno);

//            curr_time = System.currentTimeMillis(); GemStoneAddition
        }
        public long getMaxRetransmissionBurst() {
          return 0;
        }
    }


//    public static void main(String[] args) {
//        long[] xmit_timeouts = {1000, 2000, 3000, 4000};
//        AckSenderWindow win = new AckSenderWindow(new Dummy(), xmit_timeouts);
//
//
//
//        final int NUM = 1000;
//
//        for (int i = 1; i < NUM; i++)
//            win.add(i, new Message());
//
//
//        try { // GemStoneAddition
//        System.out.println(win);
//        Util.sleep(5000);
//
//        for (int i = 1; i < NUM; i++) {
//            if (i % 2 == 0) // ack the even seqnos
//                win.ack(i);
//        }
//
//        System.out.println(win);
//        Util.sleep(4000);
//        }
//        catch (InterruptedException e) {
//          return; // this is a main; just exit.
//        }
//
//        for (int i = 1; i < NUM; i++) {
//            if (i % 2 != 0) // ack the odd seqnos
//                win.ack(i);
//        }
//        System.out.println(win);
//
//        win.add(3, new Message());
//        win.add(5, new Message());
//        win.add(4, new Message());
//        win.add(8, new Message());
//        win.add(9, new Message());
//        win.add(6, new Message());
//        win.add(7, new Message());
//        win.add(3, new Message());
//        System.out.println(win);
//
//
//        try {
//            Thread.sleep(5000);
//            win.ack(5);
//            System.out.println("ack(5)");
//            win.ack(4);
//            System.out.println("ack(4)");
//            win.ack(6);
//            System.out.println("ack(6)");
//            win.ack(7);
//            System.out.println("ack(7)");
//            win.ack(8);
//            System.out.println("ack(8)");
//            win.ack(6);
//            System.out.println("ack(6)");
//            win.ack(9);
//            System.out.println("ack(9)");
//            System.out.println(win);
//
//            Thread.sleep(5000);
//            win.ack(3);
//            System.out.println("ack(3)");
//            System.out.println(win);
//
//            Thread.sleep(3000);
//            win.add(10, new Message());
//            win.add(11, new Message());
//            System.out.println(win);
//            Thread.sleep(3000);
//            win.ack(10);
//            System.out.println("ack(10)");
//            win.ack(11);
//            System.out.println("ack(11)");
//            System.out.println(win);
//
//            win.add(12, new Message());
//            win.add(13, new Message());
//            win.add(14, new Message());
//            win.add(15, new Message());
//            win.add(16, new Message());
//            System.out.println(win);
//
//            Util.sleep(1000);
//            win.ack(12);
//            System.out.println("ack(12)");
//            win.ack(13);
//            System.out.println("ack(13)");
//
//            win.ack(15);
//            System.out.println("ack(15)");
//            System.out.println(win);
//
//            Util.sleep(5000);
//            win.ack(16);
//            System.out.println("ack(16)");
//            System.out.println(win);
//
//            Util.sleep(1000);
//
//            win.ack(14);
//            System.out.println("ack(14)");
//            System.out.println(win);
//        } catch (Exception e) {
//            log.error(e);
//        }
//    }

}
