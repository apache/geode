/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class that waits for n PingRsp'es, or m milliseconds to return the initial membership
 * @author Bela Ban
 * @version $Id: PingWaiter.java,v 1.11 2005/08/11 12:43:47 belaban Exp $
 */
public class PingWaiter implements Runnable {
    public static boolean TEST_HOOK_IGNORE_REQUIRED_RESPONSE = false;
    Object tLock = new Object(); // GemStoneAddition - guards "t"
    Thread              t=null;
    final List          rsps=new LinkedList();
    long                timeout=3000;
    volatile int       num_rsps=3; // GemStoneAddition - volatile
    Protocol            parent=null;
    PingSender          ping_sender;
    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());
    private boolean     trace=log.isTraceEnabled();
    private volatile int num_servers_received; // GemStoneAddition
    private volatile Address coordinator; // GemStoneAddition
    private volatile Set<Address> requiredResponses = Collections.EMPTY_SET; // GemStoneAddition
    private volatile Set<Address> requiredResponsesReceived = new HashSet<Address>();


    public PingWaiter(long timeout, int num_rsps, Protocol parent, PingSender ping_sender) {
        this.timeout=timeout;
        this.num_rsps=num_rsps;
        this.parent=parent;
        this.ping_sender=ping_sender;
    }


    void setTimeout(long timeout) {
        this.timeout=timeout;
    }

    void setNumRsps(int num) {
        this.num_rsps=num;
    }
    
    /**
     * GemStoneAddition - bypass get_mbrs work if the gossipserver thinks 
     * it knows who the coordinator is.
     * @param theCoordinator the coordinator's address
     */
    void setCoordinator(Address theCoordinator) {
      this.coordinator = theCoordinator;
      if (theCoordinator != null) {
        this.requiredResponses = Collections.EMPTY_SET;
      }
    }


    public synchronized void start() {
        // ping_sender.start();
      synchronized (tLock) { // GemStoneAddition
        if(t == null || !t.isAlive()) {
            t=new Thread(GemFireTracer.GROUP, this, "PingWaiter");
            t.setDaemon(true);
            t.start();
        }
    }
    }

    public synchronized void stop() {
        if(ping_sender != null)
            ping_sender.stop();
        Thread thr = null;
        synchronized (tLock) { // GemStoneAddition
          thr = t;
        }
        if (thr != null) {
            thr.interrupt();
            synchronized(rsps) {
               rsps.notifyAll();
            }
        }
    }


    public synchronized boolean isRunning() {
      synchronized (tLock) {
        return t != null && t.isAlive();
      }
    }
    
    public void setRequiredResponses(Set<Address> addresses) {
      this.requiredResponses = addresses;
      this.requiredResponsesReceived.clear();
    }

    public void addResponse(PingRsp rsp) {
        if (rsp != null) {
          if (TEST_HOOK_IGNORE_REQUIRED_RESPONSE) {
            log.getLogWriter().info(ExternalStrings.DEBUG, "TEST HOOK: required responses are " + this.requiredResponses);
            if (this.requiredResponses.contains(rsp.own_addr)) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "TEST HOOK: ignoring response from " + rsp.own_addr);
              return;
            }
          }
            if (trace)
              log.trace("Received Ping response " + rsp);
            synchronized(rsps) {
                if(rsps.contains(rsp))
                    rsps.remove(rsp); // overwrite existing element
                rsps.add(rsp);
                if (rsp.is_server) { // GemStoneAddition - track number of actual members responding
                  num_servers_received++;
                }
                if (this.requiredResponses.contains(rsp.own_addr)) {
                  this.requiredResponsesReceived.add(rsp.own_addr);
                  if (trace)
                    log.trace("Received "
                      + this.requiredResponsesReceived.size() + " of "
                      + this.requiredResponses.size()
                      + " required ping responses");
                }
                rsps.notifyAll();
//log.getLogWriter().warning("Added Ping response to list");
            }
        }
    }

    public void clearResponses() {
        if (trace) {
          log.trace("<PingWaiter> clearing responses for a new attempt");
        }
        synchronized(rsps) {
            rsps.clear();
            num_servers_received = 0; // GemStoneAddition
            rsps.notifyAll();
            this.coordinator = null;
            this.requiredResponses = Collections.emptySet();
            this.requiredResponsesReceived = new HashSet<Address>();
        }
    }


    // GemStoneAddition - this method is too dangerous - it should synch on rsps and return a copy
//    public List getResponses() {
//        return rsps;
//    }



    public void run() {
        // GemStoneAddition - debugging 34274
        //log.info("findInitialMembers starting");
        Vector responses=findInitialMembers();
        //log.info("findInitialMembers found " + responses.size() + " members");
        synchronized (tLock) {
          t = null; // GemStoneAddition - need to null out so it can be started again in ClientGmsImpl's thread (bug 34274)
        }
        if(parent != null) {
          // GemStoneAddition - make sure required responses have all been received
          if (log.getLogWriter().fineEnabled()) {
            log.getLogWriter().fine("PingWaiter: required responses="+this.requiredResponses
                +"; received="+this.requiredResponsesReceived
                +"; responses="+responses);
          }
          if (this.requiredResponses.size() != this.requiredResponsesReceived.size()) {
            Set missing = new HashSet(this.requiredResponses);
            missing.removeAll(this.requiredResponsesReceived);
            if (log.getLogWriter().fineEnabled()) {
              log.getLogWriter().fine("Find Initial Members failed.  missing responses = " + missing);
            }
            parent.passUp(new Event(Event.FIND_INITIAL_MBRS_FAILED, missing));
          } else {
            if (log.getLogWriter().fineEnabled()) {
              log.getLogWriter().fine("Find Initial Members completed.");
            }
            parent.passUp(new Event(Event.FIND_INITIAL_MBRS_OK, responses));
          }
        }
    }


    @SuppressFBWarnings(value="TLW_TWO_LOCK_WAIT", justification="the code is correct")
    public Vector findInitialMembers() {
        long start_time, time_to_wait;

        synchronized(rsps) {
            if(num_servers_received >= num_rsps && rsps.size() > 0) { // GemStoneAddition - only clear if we satisfied the server count last time
                rsps.clear();
                num_servers_received = 0; // GemStoneAddition
            }

            AtomicBoolean sync = new AtomicBoolean();
            
            // GemStoneAddition - block this thread until the discovery
            // protocol is ready for the timeout to start counting down
            this.coordinator = null;
            ping_sender.setSync(sync);
            while (!sync.get()) {
              synchronized(sync) {
                clearResponses(); // GemStoneAddition - don't accumulate old junk
                ping_sender.start();
                try { sync.wait();  } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
  //log.getLogWriter().warning("findInitialMembers interrupted - returning empty set");
                  return new Vector();
                }
              }
            }

            start_time=System.currentTimeMillis();
            time_to_wait=timeout;

            try {
                // GemStoneAddition - don't stop waiting until a requisite number
                // of members is received.  Non-members don't know who their
                // coordinator really is and will be used by the GMS as a last
                // resort to elect a concurrently starting member.  In that case,
                // we want the max number of responses we can get
              Thread tTmp;
              synchronized (tLock) { // GemStoneAddition
                tTmp = t;
              }
                while((time_to_wait > 0)  &&  (tTmp != null)
                       && ((requiredResponsesReceived.size() != requiredResponses.size())
                            || (num_servers_received < num_rsps))) {
                  if (this.coordinator != null) { // GemStoneAddition - shortcut get_mbrs phase
                    PingRsp ping_rsp=new PingRsp(this.coordinator, this.coordinator, true);
                    rsps.add(ping_rsp);
                    this.coordinator = null;
                    break;
                  }

                  if(trace) // +++ remove
                        log.trace(new StringBuffer("waiting for initial members: time_to_wait=").append(time_to_wait)
                                  .append(", got ").append(rsps.size()).append(" rsps"));

                    try {
                        rsps.wait(time_to_wait);
                    }
                    catch(InterruptedException intex) {
                      Thread.currentThread().interrupt(); // GemStoneAddition
                      break; // GemStoneAddition -- treat like timeout
                    }
                    catch(Exception e) {
                        log.error(ExternalStrings.PingWaiter_GOT_AN_EXCEPTION_WAITING_FOR_RESPONSES, e);
                    }
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
                if(trace)
                    log.info(ExternalStrings.PingWaiter_INITIAL_MEMBERS_ARE_0, rsps); 
// GemStoneAddition - DEBUGGING
//log.getLogWriter().warning("time_to_wait=" + time_to_wait
//    + " num_servers_received=" + num_servers_received + " initial_mbrs=" + rsps);
                return new Vector(rsps);
            }
            finally {
//                if(ping_sender != null) GemStoneAddition (cannot be null)
                    ping_sender.stop();
            }
        }
    }
    
    // GemStoneAddition - get the coordinator that would be selected by ClientGmsImpl
    // if the concurrent selection algorithm were used for election
    public Address getPossibleCoordinator(Address local_addr) {
      Set clients=new TreeSet();
      clients.add(local_addr);
      final Vector initial_mbrs;
      synchronized (rsps) {
        initial_mbrs = new Vector(rsps);
      }
      if (initial_mbrs.size() == 0) {
        return local_addr;
      }
      for(int i=0; i < initial_mbrs.size(); i++) {
          PingRsp pingRsp=(PingRsp)initial_mbrs.elementAt(i);
          Address client_addr=pingRsp.getCoordAddress();
          if(client_addr != null)
              clients.add(client_addr);
      }
      return (Address)clients.iterator().next();
    }

}
