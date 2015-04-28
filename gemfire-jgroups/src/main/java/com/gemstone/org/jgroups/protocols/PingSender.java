/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;

import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.ShunnedAddressException;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.Util;

/**
 * Sends num_ping_request GET_MBRS_REQ messages, distributed over timeout ms
 * @author Bela Ban
 * @version $Id: PingSender.java,v 1.5 2005/08/11 12:43:47 belaban Exp $
 */
public class PingSender implements Runnable {
    // GemStoneAddition: accesses to #t must be synchronized on this.
    Thread              t=null;
//    long                timeout=3000; GemStoneAddition
    double              interval;
    int                 num_requests=1;
    Discovery           discovery_prot;
    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());
    protected boolean   trace=log.isTraceEnabled();
    private AtomicBoolean waiter_sync; // GemStoneAddition


    public PingSender(long timeout, int num_requests, Discovery d) {
//        this.timeout=timeout; GemStoneAddition
        this.num_requests=num_requests;
        this.discovery_prot=d;
        interval=timeout / (double)num_requests;
    }

    /** GemStoneAddition - a synchronization object that is used to block
        the waiter until the discovery protocol gives the go-ahead.
        This is to fix bug 34274 */
    public void setSync(AtomicBoolean waiter_sync) {
      this.waiter_sync = waiter_sync;
    }

    public synchronized void start() {
        if(t == null || !t.isAlive()) {
            t=new Thread(GemFireTracer.GROUP, this, "PingSender");
            t.setDaemon(true);
            t.start();
        }
    }

    public synchronized void stop() {
        if(t != null) {
            Thread tmp=t;
            t=null;
            try {tmp.interrupt();} catch(SecurityException ex) {}
        }
    }


    public synchronized boolean isRunning() {
        return t != null && t.isAlive();
    }



    public void run() {
        RuntimeException gfe = null;
        boolean anysuccess = false;
        
        for(int i=0; i < num_requests && !Thread.currentThread().isInterrupted(); i++) {
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
//            if(t == null || !t.equals(Thread.currentThread()))
//                break;
            if(trace)
                log.trace("sending GET_MBRS_REQ");
            try {
              // GemStoneAddition
              // bug #41355 - in case the coordinator exits during discovery,
              // only allow accelerated discovery on the first attempt 
              discovery_prot.sendGetMembersRequest(waiter_sync);
              anysuccess = true; // GemStoneAddition
              try { // GemStoneAddition
                Util.sleep((long)interval);
              }
              catch (InterruptedException e) {
                break; // exit the loop and the thread
              }
            }
            catch (ShunnedAddressException ex) {
              gfe = ex;
            }
            catch (RuntimeException ex) {
              if (ex.getClass().getSimpleName().equals("GemFireConfigException")) {
                gfe = ex;
              } else {
                throw ex;
              }
            }
        }
        // fix for bug 36066 - only pass up an exit event if
        // we never contact a locator
        if (!anysuccess) {
          discovery_prot.passUp(new Event(Event.EXIT, gfe));
          discovery_prot.wakeWaiter(waiter_sync);  // GemStoneAddition
        }
    }
}
