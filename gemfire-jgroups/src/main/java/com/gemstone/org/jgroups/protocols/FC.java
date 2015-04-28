/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FC.java,v 1.50 2005/10/28 14:46:49 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import java.util.concurrent.ConcurrentHashMap;
//import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.BoundedList;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender. Works for both unicast and multicast messages.
 * <p>
 * Note that this protocol must be located towards the top of the stack, or all down_threads from JChannel to this
 * protocol must be set to false ! This is in order to block JChannel.send()/JChannel.down().
 * <br/>This is the second simplified implementation of the same model. The algorithm is sketched out in
 * doc/FlowControl.txt
 * @author Bela Ban
 * @version $Revision: 1.50 $
 */
public class FC extends Protocol  {
  
    /** HashMap key=Address, value=Long: keys are members, values are credits left. For each send, the
     * number of credits is decremented by the message size */
    final Map sent=new HashMap(11);
    // final Map sent=new ConcurrentHashMap(11);

    /* Throttle request from the receivers; key receivers, values throttle(sleep) time */
    final Map throttle=new HashMap(11);

    /** HashMap key=Address, value=Long: keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits are 0, we refill and send a CREDIT message to the sender. Sender blocks until CREDIT
     * is received after reaching <tt>min_credits</tt> credits. */
    final Map received=new ConcurrentHashMap(11);
    // final Map received=new ConcurrentHashMap(11);


    /** List of members from whom we expect credits */
    final List creditors=new ArrayList(11);

    /** Max number of bytes to send per receiver until an ack must
     * be received before continuing sending */
    private long max_credits=50000;
    private Long max_credits_constant;

    /** Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to
     * wait forever.
     */
    private long max_block_time=5000;

    /** If credits fall below this limit, we send more credits to the sender. (We also send when
     * credits are exhausted (0 credits left)) */
    private double min_threshold=0.25;

    /** Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation */
    private long min_credits;

    /** Whether FC is still running, this is set to false when the protocol terminates (on stop()) */
    private boolean running=true;

    /** Determines whether or not to block on down(). Set when not enough credit is available to send a message
     * to all or a single member */
    private boolean insufficient_credit;

    /** the lowest credits of any destination (sent_msgs) */
    private long lowest_credit=max_credits;

    /** Mutex to block on down() */
    volatile Object mutex = new Object();

    static final String name="FC";

//    private long start_blocking;

    private int num_blockings;
    private int num_credit_requests_received, num_credit_requests_sent;
    private int num_credit_responses_sent, num_credit_responses_received;
    private long total_time_blocking;

    private long mcast_throttle_time;
    private Object mcast_throttle_mutex = new Object();
    
    final BoundedList last_blockings=new BoundedList(50);
    
    final Map throttledSenders = new HashMap();
    
    
//    Address coordinator; // current view coordinator GemStoneAddition (omitted)
    
//    private Address local_addr= null; // GemStoneAddition

//    final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
//    final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);

    @Override // GemStoneAddition
    public final String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumFC;
    }
    // end GemStone addition


    @Override // GemStoneAddition
    public void resetStats() {
        super.resetStats();
        num_blockings=0;
        num_credit_responses_sent=num_credit_responses_received=num_credit_requests_received=num_credit_requests_sent=0;
        total_time_blocking=0;
        last_blockings.removeAll();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
        max_credits_constant=Long.valueOf(this.max_credits);
    }

    public double getMinThreshold() {
        return min_threshold;
    }

    public void setMinThreshold(double min_threshold) {
        this.min_threshold=min_threshold;
    }

    public long getMinCredits() {
        return min_credits;
    }

    public void setMinCredits(long min_credits) {
        this.min_credits=min_credits;
    }

    public boolean isBlocked() {
        return insufficient_credit;
    }

    public int getNumberOfBlockings() {
        return num_blockings;
    }

    public long getMaxBlockTime() {
        return max_block_time;
    }

    public void setMaxBlockTime(long t) {
        max_block_time=t;
    }

    public long getTotalTimeBlocked() {
        return total_time_blocking;
    }

    public double getAverageTimeBlocked() {
        return num_blockings == 0? 0.0 : total_time_blocking / (double)num_blockings;
    }

    public int getNumberOfCreditRequestsReceived() {
        return num_credit_requests_received;
    }

    public int getNumberOfCreditRequestsSent() {
        return num_credit_requests_sent;
    }

    public int getNumberOfCreditResponsesReceived() {
        return num_credit_responses_received;
    }

    public int getNumberOfCreditResponsesSent() {
        return num_credit_responses_sent;
    }

    public String printSenderCredits() {
        return printMap(sent);
    }

    public String printReceiverCredits() {
        return printMap(received);
    }

    public String printCredits() {
        StringBuffer sb=new StringBuffer();
        sb.append("senders:\n").append(printMap(sent)).append("\n\nreceivers:\n").append(printMap(received));
        return sb.toString();
    }

    @Override // GemStoneAddition
    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        retval.put("senders", printMap(sent));
        retval.put("receivers", printMap(received));
        retval.put("num_blockings", Integer.valueOf(this.num_blockings));
        retval.put("avg_time_blocked", Double.valueOf(getAverageTimeBlocked()));
        retval.put("num_replenishments", Integer.valueOf(this.num_credit_responses_received));
        retval.put("total_time_blocked", Long.valueOf(total_time_blocking));
        return retval;
    }

    public String showLastBlockingTimes() {
        return last_blockings.toString();
    }



    /** Allows to unblock a blocked sender from an external program, e.g. JMX */
    @SuppressFBWarnings(value="IL_INFINITE_RECURSIVE_LOOP", justification="the code is correct")
    public void unblock() {
      Object mux = mutex;
        synchronized(mux) {
          if (mutex != mux) { // GemFire bug 40243
            unblock();
            return;
          }
            if(trace)
                log.trace("unblocking the sender and replenishing all members, creditors are " + creditors);

            Map.Entry entry;
            for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                entry.setValue(max_credits_constant);
            }

            lowest_credit=computeLowestCredit(sent);
            creditors.clear();
            insufficient_credit=false;
            mux.notifyAll();
        }
    }



    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String  str;
        boolean min_credits_set=false;

        super.setProperties(props);
        str=props.getProperty("max_credits");
        if(str != null) {
            max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        str=props.getProperty("min_threshold");
        if(str != null) {
            min_threshold=Double.parseDouble(str);
            props.remove("min_threshold");
        }

        str=props.getProperty("min_credits");
        if(str != null) {
            min_credits=Long.parseLong(str);
            props.remove("min_credits");
            min_credits_set=true;
        }

        if(!min_credits_set)
            min_credits=(long)(/*(double) GemStoneAddition */max_credits * min_threshold);

        str=props.getProperty("max_block_time");
        if(str != null) {
            max_block_time=Long.parseLong(str);
            props.remove("max_block_time");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.FC_FCSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        max_credits_constant=Long.valueOf(max_credits);
        return true;
    }

    @Override // GemStoneAddition
    public void start() throws Exception {
        super.start();
        Object mux = mutex;
        synchronized(mux) {
          if (mutex != mux) { // GemFire bug 40243
            start();
            return;
          }
            running=true;
            insufficient_credit=false;
            lowest_credit=max_credits;
        }
    }

    @Override // GemStoneAddition
    public void stop() {
        super.stop();
        Object mux = mutex;
        synchronized(mux) {
          if (mutex != mux) { // GemFire bug 40243
            stop();
            return;
          }
            running=false;
            mutex.notifyAll();
        }
    }


    /**
     * We need to receive view changes concurrent to messages on the down events: a message might blocks, e.g.
     * because we don't have enough credits to send to member P. However, if member P crashed, we need to unblock !
     * @param evt
     */
//    protected void receiveDownEvent(Event evt) {
//        if(evt.getType() == Event.VIEW_CHANGE) {
//            View v=(View)evt.getArg();
//            Vector mbrs=v.getMembers();
//            handleViewChange(mbrs);
//        }
//        super.receiveDownEvent(evt);
//    }

    @Override // GemStoneAddition
    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.MSG:
            handleDownMessage(evt);
            return;
        case Event.VIEW_CHANGE: // GemStoneAddition - moved from receiveDownEvent
          View v=(View)evt.getArg();
          Vector mbrs=v.getMembers();
          handleViewChange(mbrs);
          break;
        }
        passDown(evt); // this could potentially use the lower protocol's thread which may block
    }




    @Override // GemStoneAddition
    public void up(Event evt) {
        switch(evt.getType()) {

//            case Event.SET_LOCAL_ADDRESS: // GemStoneAddition
//              local_addr = (Address)evt.getArg();
//              break;

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FcHeader hdr=(FcHeader)msg.removeHeader(name);
                if(hdr != null) {
                    Address sender=msg.getSrc();
                    switch(hdr.type) {
                    case FcHeader.REPLENISH:
                      if (log.isTraceEnabled())
                        log.trace("(FC) received REPLENISH from "+sender+" with " + hdr.getBalance());
                        num_credit_responses_received++;
                        handleCredit(msg.getSrc(), hdr.getBalance()); // GemStoneAddition
                        break;
                    case FcHeader.THROTTLE:
                      if (log.isTraceEnabled())
                        log.trace("(FC) received THROTTLE request from "+sender+" with " + hdr.getBalance());
                      handleThrottleRequest(msg.getSrc(), hdr.getBalance()); // GemStoneAddition
                      break;
                    case FcHeader.WAIT: // GemStoneAddition
                      if (log.isTraceEnabled())
                        log.trace("(FC) received WAIT request from "+sender+". Responding with credit request");
                        sendCreditRequest(msg.getSrc());
                        break;
                    case FcHeader.CREDIT_REQUEST:
                        /*
                        if (stack.jgmm.isSerialQueueThrottled(sender)) { // GemStoneAddition
                          sendNoCredit(sender);
                          stack.gemfireStats.incJg3(1);
                          break;
                        }*/
                        
                        num_credit_requests_received++;
                        
                        long balance = ((Long)received.get(sender)).longValue(); // GemStoneAddition
                        received.put(sender, max_credits_constant);
                        if (log.isTraceEnabled())
                          log.trace("(FC) received credit request from "+sender+": sending "+(max_credits-balance)+" credits");
                        sendCredit(sender, max_credits - balance);
                        stack.gfPeerFunctions.incFlowControlResponses(); // GemStoneAddition
                        break;
                    default:
                        log.error(ExternalStrings.FC_HEADER_TYPE__0__NOT_KNOWN, hdr.type);
                        break;
                    }
                    return; // don't pass message up
                }
                else {
                    adjustCredit(msg);
                }

                break;

        case Event.VIEW_CHANGE:
            View newView = (View)evt.getArg();
//            this.coordinator = newView.getCreator(); GemStoneAddition
            handleViewChange(newView.getMembers());
            break;
        }
        passUp(evt);
    }


    @SuppressFBWarnings(value="IMSE_DONT_CATCH_IMSE", justification="the code is for a hotspot bug")
    private void handleDownMessage(Event evt) {
      boolean requestSent = false;
        Message msg=(Message)evt.getArg();
        if (msg.isHighPriority || Thread.currentThread().getName().startsWith("UDP")) {
          passDown(evt);
          return;
        }
        int     length=msg.getLength();
        Address dest=msg.getDest();
        long blockStartTime = 0; // GemStoneAddition - statistics
        
        // See if there was a throttle request from the receiever.
        throttleOnReceiver(dest); // GemStoneAddition
        try {
          Object mux = mutex;
          synchronized(mux) {
            if (mux != mutex) { // GemFire bug 40243 hit in another thread
              passDown(evt);
              return;
            }
            if(lowest_credit <= length) {
              stack.gfPeerFunctions.incJgFCsendBlocks(1);
              determineCreditors(dest, length);
              insufficient_credit=true;
              num_blockings++;
              blockStartTime = stack.gfPeerFunctions.startFlowControlWait(); // GemStoneAddition - statistics
              long start_blocking=System.currentTimeMillis();
              boolean warned = false;
              boolean shunned = false;
              while(insufficient_credit && running
                  && creditors.size() > 0  /* GemStoneAddition */) {
                try {mux.wait(max_block_time);} catch(InterruptedException e) {
                  Thread.currentThread().interrupt(); // GemStoneAddition
                  break; //  GemStoneAddition
                }
                if(insufficient_credit && running) {
                  stack.gfPeerFunctions.incJgFCautoRequests(1);
                  int secs = stack.gfPeerFunctions.getAckWaitThreshold();
                  long elapsed =System.currentTimeMillis()-start_blocking;
                  if (elapsed > secs * 1000) {
                    if (!warned) {
                      warned = true;
                      log.getLogWriter().warning(
                          ExternalStrings.FC_FLOW_CONTROL_HAS_BLOCKED_FOR_MORE_THAN_0_SECONDS_WAITING_FOR_REPLENISHMENT_FROM_1,
                          new Object[] {Long.valueOf(elapsed/1000), creditors});
                    }
                    else {
                      secs = stack.gfPeerFunctions.getAckSevereAlertThreshold();
                      if (secs > 0 && !shunned && elapsed > secs * 1000) {
                        shunned = true;
                        //warned = false; // allow another warning so we can see if shunning worked
                        /*for (Iterator it=creditors.iterator(); it.hasNext(); ) {
                                Address badmbr = (Address)it.next();
                                Message shun = new Message();
                                shun.setDest(this.coordinator);
                                GMS gms = (GMS)stack.findProtocol("GMS");
                                shun.putHeader(gms.getName(), new GMS.GmsHeader(
                                    GMS.GmsHeader.REMOVE_REQ, badmbr));
                                passDown(new Event(Event.MSG, shun));
                              }*/
                      }
                    }
                  }
                  if (!requestSent) { // UNICAST will retransmit if someone doesn't get the message
                    requestSent = true;
                    for(int i=0; i < creditors.size(); i++) {
                      sendCreditRequest((Address)creditors.get(i));
                    }
                  }
                }
              }
              if (warned) {
                log.getLogWriter().warning(
                    ExternalStrings.FC_FLOW_CONTROL_WAS_UNBLOCKED_AFTER_WAITING_0_SECONDS,
                    Long.valueOf((System.currentTimeMillis()-start_blocking)/1000));
              }
              stack.gfPeerFunctions.endFlowControlWait(blockStartTime);
              //stop_blocking=System.currentTimeMillis();
              //long block_time=stop_blocking - start_blocking;
              //if(trace)
              //    log.trace("total time blocked: " + block_time + " ms");
              //total_time_blocking+=block_time;
              //last_blockings.add(Long.valueOf(block_time));
            }
            else {
              long tmp=decrementCredit(sent, dest, length);
              if(tmp != -1)
                lowest_credit=Math.min(tmp, lowest_credit);
            }
          }
        } catch (IllegalMonitorStateException e) {
          // GemFire bug 40243 is a problem with the hotspot compiler corrupting the
          // lock of an object.  In this case, it's the mutex object and we
          // can replace it & have another go
          mutex = new Object();
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        passDown(evt);
        
    }

    /**
     * Checks whether one member (unicast msg) or all members (multicast msg) have enough credits. Add those
     * that don't to the creditors list
     * @param dest
     * @param length
     */
    private void determineCreditors(Address dest, int length) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        Address mbr;
        Long    credits;
        if(multicast) {
            Map.Entry entry;
            for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                mbr=(Address)entry.getKey();
                credits=(Long)entry.getValue();
                if(credits.longValue() <= length) {
                    if(!creditors.contains(mbr))
                        creditors.add(mbr);
                }
            }
        }
        else {
            credits=(Long)sent.get(dest);
            if(credits != null && credits.longValue() <= length) {
                if(!creditors.contains(dest))
                    creditors.add(dest);
            }
        }
    }


    /**
     * Decrements credits from a single member, or all members in sent_msgs, depending on whether it is a multicast
     * or unicast message. No need to acquire mutex (must already be held when this method is called)
     * @param dest
     * @param credits
     * @return The lowest number of credits left, or -1 if a unicast member was not found
     */
    private long decrementCredit(Map m, Address dest, long credits) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        long    lowest=max_credits, tmp;
        Long    val;

        if(multicast) {
            if(m.size() == 0)
                return -1;
            Map.Entry entry;
            for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                val=(Long)entry.getValue();
                tmp=val.longValue();
                tmp-=credits;
                entry.setValue(Long.valueOf(tmp));
                lowest=Math.min(tmp, lowest);
            }
            return lowest;
        }
        else {
            val=(Long)m.get(dest);
            if(val != null) {
                lowest=val.longValue();
                lowest-=credits;
                m.put(dest, Long.valueOf(lowest));
                return lowest;
            }
        }
        return -1;
    }

    /**
     * Handle throttle message from a peer(receiver)
     * 
     * @param sender
     * @param balance thrrottle(sleep) time on this receiver. 
     */
    private void handleThrottleRequest(Address sender, long balance) { // GemStoneAddition
        if(sender == null) {return;}       
        synchronized(throttle) {
          // The individual throttling used for p2p send.
          throttle.put(sender, Long.valueOf(balance));
          // Used for mcast send, it uses the max throttle time between the
          // sender. Once we implement the slow-receiver handling capability
          // we need to change the following code.
          if (balance > mcast_throttle_time) {
            mcast_throttle_time = balance;
          }
        }
    }
    
    /**
     * Throttles on the receiver. //GemStoneAddition
     * @param dest
     */
    private void throttleOnReceiver(Address dest) {
      
      boolean multicast=dest == null || dest.isMulticastAddress();
      
      int sleep = 0;
      Long p2p_throttle = null;
      long blockStartTime = 0; 
      //if (stack.enableClockStats)
      
      // Possible multithread (multi-sender) casses:
      // 1. multiple mcast threads.
      // 2. multiple p2p threads
      // 3. While mcast thread throttling, p2p thread arrives.
      // 4. While p2p thread throttling, mcast thread arrives.
            
      if (multicast){
        synchronized(throttle) {
          if (mcast_throttle_time <= 0) {return;}    
          sleep = (int)mcast_throttle_time;
          mcast_throttle_time = 0;
        }
        
        // block other mcast sender-threads.
        synchronized(mcast_throttle_mutex) {
          if (trace) log.trace("### throttling for mcast" + sleep);
          blockStartTime = stack.gfPeerFunctions.startFlowControlThrottleWait();
          throttleSleep(sleep);
          stack.gfPeerFunctions.endFlowControlThrottleWait(blockStartTime);
        }
        
        throttle.clear();
      }
      else {

        p2p_throttle = ((Long)throttle.get(dest));
        if (p2p_throttle != null) {
          // Throttle other p2p threads on this sender.
          synchronized (p2p_throttle){
            blockStartTime = stack.gfPeerFunctions.startFlowControlThrottleWait();
            sleep = p2p_throttle.intValue();
            throttleSleep(sleep);
            stack.gfPeerFunctions.endFlowControlThrottleWait(blockStartTime);
          }
        
          throttle.remove(dest);
        
          // During this time if there is no mcast sender, decrement this sleep
          // time from the mcast throttle time.
          if (mcast_throttle_time > 0){
            mcast_throttle_time -= sleep;
          }
        }
      }
    }
    
    /**
     * Throttles on the receiver. // GemStoneAddition
     * @param sleep
     */
    private void throttleSleep(int sleep) {
      try {          
        Thread.sleep(sleep);
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        // propagated the bit.  Let the caller deal with it.
      }      
    }

    /**
     * Handle replenish message from a peer
     * 
     * @param sender
     * @param balance amount the sender allows us to replenish by
     */
    @SuppressFBWarnings(value="IL_INFINITE_RECURSIVE_LOOP", justification="the code is correct")
    private void handleCredit(Address sender, long balance) { // GemStoneAddition
        if(sender == null) return;
        StringBuffer sb=null;

        Object mux = mutex;
        synchronized(mutex) {
          if (mutex != mux) { // GemFire bug 40243
            handleCredit(sender, balance);
            return;
          }
            Long old_entry =(Long)sent.get(sender);// GemStoneAddition
            if (old_entry == null) {
              // we don't know about this person.  How can this happen?
              // My best guess is that since our messages aren't totally ordered,
              // the peer has departed, but his replenishment request has already
              // been received.
              //
              // If this is indeed what is happening, it's safe to
              // just ignore this.  2006-05-22
              return;
            }
            long old_credit=old_entry.longValue(); // GemStoneAddition
            stack.gfPeerFunctions.incJgFCreplenish(1); // GemStoneAddition
            if(trace) {
                sb=new StringBuffer();
                sb.append("received credit <" + balance + "> from ").append(sender).append(", old credit was ").
                        append(old_credit).append(", new credits are ").append(max_credits).
                        append(".\nCreditors before are: ").append(creditors);
            }

            // GemStoneAddition
            old_credit += balance;
            if (old_credit > max_credits) {
              old_credit = max_credits;
            }
            sent.put(sender, Long.valueOf(old_credit)); // replenish
            
            // this sender is no longer a creditor.
            if(creditors.size() > 0) {  // we are blocked because we expect credit from one or more members
                creditors.remove(sender);
                if(trace) {
                    sb.append("\nCreditors after removal of ").append(sender).append(" are: ").append(creditors);
                    log.trace(sb.toString());
                }
            }

            lowest_credit=computeLowestCredit(sent);
            if(insufficient_credit && lowest_credit > 0 && creditors.size() == 0) {
                insufficient_credit=false;
                mutex.notifyAll();
                stack.gfPeerFunctions.incJgFCresumes(1);
            }
        }
    }

    private long computeLowestCredit(Map m) {
        Collection credits=m.values(); // List of Longs (credits)
        Long retval=(Long)Collections.min(credits);
        return retval.longValue();
    }


    /**
     * Check whether sender has enough credits left. If not, send him some more
     * @param msg
     */
    private void adjustCredit(Message msg) {
        Address src=msg.getSrc();
        long    length=msg.getLength(); // we don't care about headers for the purpose of flow control

        if(src == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.FC_SRC_IS_NULL);
            return;
        }

        if(length == 0)
            return; // no effect

        // GemStoneAddition
        // We have a unicast address here, so determine the balance for
        // this particular sender
        long balance = decrementCredit(received, src, length);
        
        int throttleTime = 0;
        if (msg.isCacheOperation) {
          boolean check = (msg.getDest() == null || msg.getDest().isMulticastAddress());
          check = check || stack.gfPeerFunctions.getDisableTcp();
          if (check) {
            throttleTime = stack.gfPeerFunctions.getSerialQueueThrottleTime(src);
          }
        }

        if (throttleTime > 0) {  // GemStoneAddition throttling
          boolean sendMsg = false;
          synchronized(throttledSenders) {
            Long throttleStart = (Long)throttledSenders.get(src);
            // only send throttles once per second to avoid bombarding the other process
            long now = System.currentTimeMillis();
            if (throttleStart == null || (now-throttleStart) > 1000) {
              throttledSenders.put(src, Long.valueOf(now));
              sendMsg = true;
            }
          }
          if (sendMsg) {
            sendThrottleRequest(src, throttleTime);
          }
        }        

        else if(balance <= min_credits) {
          synchronized(throttledSenders) { // GemStoneAddition - throttling
            throttledSenders.remove(src);
          }
            received.put(src, max_credits_constant);
            if(trace) log.trace("sending replenishment message to " + src + " for " + msg);
            sendCredit(src, max_credits - balance); // GemStoneAddition
        }
    }

    /**
     * Send a throttle message to a peer (sender)
     * If the serial queue (executor) is unable to keep-up with incoming message
     * a throttle request is sent to the sender, so that the messages are not 
     * lived in the serial-queued executor for long (added as part of bugfix:35268).
     * @param dest
     * @param throttleTime - the time to throttle(sleep) GemStoneAddition
     */
    private void sendThrottleRequest(Address dest, long throttleTime) {
        stack.gfPeerFunctions.incJgFCsentThrottleRequests(1); // GemStoneAddition

        Message  msg=new Message(dest, null, null);
        msg.putHeader(name, new FcHeader(FcHeader.THROTTLE, throttleTime));
        msg.isHighPriority = true;
        passDown(new Event(Event.MSG, msg));   
    }


    /**
     * Send a replenish message to a peer
     * 
     * @param dest
     * @param credit - the amount of credit to send GemStoneAddition
     */
    private void sendCredit(Address dest, long credit) {
        stack.gfPeerFunctions.incJgFCsentCredits(1); // GemStoneAddition
        Message  msg=new Message(dest, null, null);
        msg.putHeader(name, new FcHeader(FcHeader.REPLENISH, credit));
        msg.isHighPriority = true;
        passDown(new Event(Event.MSG, msg));
        num_credit_responses_sent++;
    }

//    /** GemStoneAddition - send a message telling other process that it's not getting more credits
//        right now */
//    private void sendNoCredit(Address dest) {
//        Message  msg=new Message(dest, null, null);
//        msg.putHeader(name, new FcHeader(FcHeader.WAIT, 0));
//        msg.bundleable = false;
//        passDown(new Event(Event.MSG, msg));
//    }

    private void sendCreditRequest(final Address dest) {
        Message  msg=new Message(dest, null, null);
        
        // No balance sent on a credit request.  Perhaps we should???
        msg.putHeader(name, new FcHeader(FcHeader.CREDIT_REQUEST, 0)); // GemStoneAddition
        
        msg.isHighPriority = true;
        passDown(new Event(Event.MSG, msg));
        num_credit_requests_sent++;
        stack.gfPeerFunctions.incFlowControlRequests(); // GemStoneAddition
    }


    @SuppressFBWarnings(value="IL_INFINITE_RECURSIVE_LOOP", justification="the code is correct")
    private void handleViewChange(Vector mbrs) {
        Address addr;
        if(mbrs == null) return;
        //if(trace) log.trace("new membership: " + mbrs);

        Object mux = mutex;
        synchronized(mutex) {
          if (mutex != mux) { // GemFire bug 40243
            handleViewChange(mbrs);
            return;
          }
            // add members not in membership to received and sent hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=(Address) mbrs.elementAt(i);
                if(!received.containsKey(addr))
                    received.put(addr, max_credits_constant);
                if(!sent.containsKey(addr))
                    sent.put(addr, max_credits_constant);
            }
            // remove members that left
            for(Iterator it=received.keySet().iterator(); it.hasNext();) {
                addr=(Address) it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }

            // remove members that left
            for(Iterator it=sent.keySet().iterator(); it.hasNext();) {
                addr=(Address)it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }

            // remove all creditors which are not in the new view
            for(int i=0; i < creditors.size(); i++) {
                Address creditor=(Address)creditors.get(i);
                if(!mbrs.contains(creditor))
                    creditors.remove(creditor);
            }

            if(trace) log.trace("creditors are " + creditors);
            if(insufficient_credit && creditors.size() == 0) {
                lowest_credit=computeLowestCredit(sent);
                insufficient_credit=false;
                mutex.notifyAll();
            }
        }
    }

    private static String printMap(Map m) {
        Map.Entry entry;
        StringBuffer sb=new StringBuffer();
        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }





    public static class FcHeader extends Header implements Streamable {
        public static final byte REPLENISH      = 1;
        public static final byte CREDIT_REQUEST = 2; // the sender of the message is the requester
        public static final byte WAIT = 3; // GemStoneAddition
        public static final byte THROTTLE      = 4;
        // GemStoneAddition
        /**
         * In the event of a REPLENISH request, the balance is the number of
         * credits that we permit the sender to actually put to his account.
         */
        long balance;
        
        byte  type = REPLENISH;

        public FcHeader() {

        }

        public FcHeader(byte type, long balance) {
            this.type=type;
            this.balance = balance; // GemStoneAddition
        }

        public long getBalance() { return balance; } // GemStoneAddition
        
        @Override // GemStoneAddition
        public long size(short version) {
            return Global.BYTE_SIZE + Global.LONG_SIZE; // GemStoneAddition
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeLong(balance); // GemStoneAddition
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            balance=in.readLong(); // GemStoneAddition
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(balance); // GemStoneAddition
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            balance=in.readLong(); // GemStoneAddition
        }

        @Override // GemStoneAddition
        public String toString() {
            switch(type) {
            case REPLENISH: return "REPLENISH";
            case CREDIT_REQUEST: return "CREDIT_REQUEST";
            case WAIT: return "WAIT";
            case THROTTLE: return "THROTTLE";
            default: return "<invalid type>";
            }
        }
    }


}
