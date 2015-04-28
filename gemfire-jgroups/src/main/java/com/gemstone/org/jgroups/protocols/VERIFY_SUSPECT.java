/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: VERIFY_SUSPECT.java,v 1.16 2005/12/16 16:08:17 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
//import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;
import com.gemstone.org.jgroups.util.GemFireTracer;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;


/**
 * Catches SUSPECT events traveling up the stack. Verifies that the suspected member is really dead. If yes,
 * passes SUSPECT event up the stack, otherwise discards it. Has to be placed somewhere above the FD layer and
 * below the GMS layer (receiver of the SUSPECT event). Note that SUSPECT events may be reordered by this protocol.
 */
public class VERIFY_SUSPECT extends Protocol implements Runnable {
    private static int MIN_SLEEP_TIME = 500;  // GemStoneAddition - minimum sleep time in run() loop
    private Address     local_addr=null;
    protected             long timeout=2000;   // number of millisecs to wait for an are-you-dead msg
    private             int num_msgs=1;     // number of are-you-alive msgs and i-am-not-dead responses (for redundancy)
    // GemStoneAddition: accesses to suspect all synchronized on the instance.
    final Hashtable     suspects=new Hashtable();  // keys=Addresses, vals=time in mcses since added
    boolean suspectsAdded; // GemStoneAddition - bug #44857, guarded by suspects
    private Thread timer=null; // GemStoneAddition - synchronized(this) to access
    static final String name="VERIFY_SUSPECT";
    
    private boolean playingDead; // GemStoneAddition - test hook for ack-severe-alert-threshold testing
    
    /**
     * GemStoneAddition - cache the view here so we can avoid suspecting 
     * members not in the view.  Use {@link #view_lock} to guard access to this
     * object.
     */
    private View view;
    
    /**
     * GemStoneAddition - guard for view
     */
    private Object view_lock = new Object();
    /**
     * GemStoneAddition - disables processing when shutdown is in progress
     */
    private boolean disconnecting;

    /** GemStoneAddition - suspect-fast timeout period */
    static final long SUSPECT_FAST_TIMEOUT = Integer.getInteger("gemfire.fast-member-timeout", 1000).intValue();

    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("num_msgs");
        if(str != null) {
            num_msgs=Integer.parseInt(str);
            if(num_msgs <= 0) {
                if(warn) log.warn("num_msgs is invalid (" +
                        num_msgs + "): setting it to 1");
                num_msgs=1;
            }
            props.remove("num_msgs");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.VERIFY_SUSPECT_VERIFY_SUSPECTSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        this.disconnecting = false; // no longer disconnecting
        return true;
    }
    
    /**
     * GemStoneAddition suspected members processing from GMS view processing
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
      switch (evt.getType()) {
      case Event.VIEW_CHANGE:
        synchronized(view_lock) {
          this.view = (View)evt.getArg();
        }
        break;

      case Event.DISCONNECTING: // GemStoneAddition - stop processing when disconnecting
        stop();
        this.disconnecting = true;
        break;
        
      }
      
      passDown(evt);
    }

    @Override // GemStoneAddition  
    public void up(Event evt) {
//        Address suspected_mbr;
        Message msg, rsp;
        Object obj;
        VerifyHeader hdr;

        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
            
        case Event.VIEW_CHANGE:
          // GemStoneAddition - it's been common for suspicion to linger after
          // a member has been declared dead.  This stops suspect processing
          // on members that are known to no longer be in the view.
          View v = (View)evt.getArg();
          long viewId = v.getVid().getId();
          Set<IpAddress> removals = new HashSet<IpAddress>();
          synchronized(suspects) {
            for (Iterator it=suspects.keySet().iterator(); it.hasNext(); ) {
              IpAddress mbr = (IpAddress)it.next();
              if (!v.containsMember(mbr) && mbr.getBirthViewId() < viewId) {
                removals.add(mbr);
              }
            }
          }
          for (IpAddress mbr: removals) {
            unsuspect(mbr);
          }
          break;

        case Event.SUSPECT:  // it all starts here ...
          if (this.disconnecting) break; // GemStoneAddition
          SuspectMember sm = (SuspectMember)evt.getArg(); // GemStoneAddition - SuspectMember struct
            if(sm == null || sm.suspectedMember == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.VERIFY_SUSPECT_SUSPECTED_MEMBER_IS_NULL);
                return;
            }
            suspect(sm);
            return;  // don't pass up; we will decide later (after verification) whether to pass it up


        case Event.MSG:
            msg=(Message)evt.getArg();
            obj=msg.getHeader(name);
            // GemStoneAddition - any traffic from a suspect member is counted as "i am not dead"
            if (msg.getSrc() != null  &&  ((IpAddress)msg.getSrc()).getBirthViewId() >= 0) {
              unsuspect(msg.getSrc());
            }
            if(obj == null || !(obj instanceof VerifyHeader))
                break;
            if (this.disconnecting) return; // GemStoneAddition
            hdr=(VerifyHeader)msg.removeHeader(name);
            switch(hdr.type) {
            case VerifyHeader.ARE_YOU_DEAD:
                if(hdr.from == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.VERIFY_SUSPECT_ARE_YOU_DEAD_HDRFROM_IS_NULL);
                }
                else {
                  // GemStoneAddition test hook
                  if (playingDead) {
                    return;
                  }
                    for(int i=0; i < num_msgs; i++) {
                        rsp=new Message(hdr.from, local_addr, null);
                        rsp.putHeader(name, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                        passDown(new Event(Event.MSG, rsp));
                    }
                }
                return;
            case VerifyHeader.I_AM_NOT_DEAD:
                if(hdr.from == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.VERIFY_SUSPECT_I_AM_NOT_DEAD_HDRFROM_IS_NULL);
                    return;
                }
                unsuspect(hdr.from);
                return;
            }
            return;
        }
        passUp(evt);
    }
    
    
    /** GemStoneAddition - test hook */
    public void playDead(boolean flag) {
      this.playingDead = flag;
    }


    /**
     * Will be started when a suspect is added to the suspects hashtable. Continually iterates over the
     * entries and removes entries whose time have elapsed. For each removed entry, a SUSPECT event is passed
     * up the stack (because elapsed time means verification of member's liveness failed). Computes the shortest
     * time to wait (min of all timeouts) and waits(time) msecs. Will be woken up when entry is removed (in case
     * of successful verification of that member's liveness). Terminates when no entry remains in the hashtable.
     */
    public void run() {
        Address mbr;
        long val, curr_time, diff;

        for (;;) { // GemStoneAddition remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition (for safety?)
          
          if (!stack.getChannel().isOpen()) break; // GemStoneAddition
          
            diff=0;
            if (suspects.size() <= 0)
              break; // GemStoneAddition

            ArrayList msgs = new ArrayList();
            long sleepTime = timeout;
            Set<SuspectMember> passUps = new HashSet<SuspectMember>();
            synchronized(suspects) {
              this.suspectsAdded = false;
                  for(Enumeration e=suspects.keys(); e.hasMoreElements();) {
                    SuspectMember sm =(SuspectMember)e.nextElement(); // GemStoneAddition - SuspectMember struct
                    mbr = sm.suspectedMember;
                    val=((Long)suspects.get(sm)).longValue();
//                    boolean fast = (val < 0);
//                    if (fast) {
//                      val = -val;
//                    }
                    curr_time=System.currentTimeMillis();
                    diff=curr_time - val;
                    if(diff >= timeout) {  // haven't been unsuspected, pass up SUSPECT
                      // GemStoneAddition logging
//                      if (trace) {
                        log.getLogWriter().info(ExternalStrings.DEBUG,
                            ""+this.local_addr+": No suspect verification response received from " + mbr
                          + " in " + diff + " milliseconds: I believe it is gone.");
//                      }
//                        if(trace)
//                            log.trace("diff=" + diff + ", mbr " + mbr + " is dead (passing up SUSPECT event)");
                        passUps.add(sm);
                        suspects.remove(sm);
                        continue;
                    }
                    else { // GemStoneAddition - send another are-you-dead message now
                      Message msg=new Message(mbr, local_addr, null);
                      msg.putHeader(name, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
                      msgs.add(msg);
                      sleepTime = Math.min(sleepTime, timeout-diff);
                    }
                    diff=Math.max(MIN_SLEEP_TIME, sleepTime);
                }
            }
            // GemStoneAddition - do notification out of sync to avoid deadlock
            for (SuspectMember sm: passUps) {
              passUp(new Event(Event.SUSPECT, sm));
            }
            
            // GemStoneAddition - do messaging outside of sync to avoid deadlock
            // with TP
            for (Iterator it=msgs.iterator(); it.hasNext(); ) {
              if (!disconnecting) { // GemStoneAddition - don't suspect when disconnecting
                passDown(new Event(Event.MSG, it.next()));
              }
            }

            // can't sleep longer than the suspect_fast_timeout in case
            // a new fast_suspect gets added to the collection
//            diff = Math.min(SUSPECT_FAST_TIMEOUT, diff);
            
            if(sleepTime > 0) {
              synchronized(suspects) { // GemStoneAddition - bug #44857
                if (!suspectsAdded) {
                  try { // GemStoneAddition
                    suspects.wait(sleepTime);
                  }
                  catch (InterruptedException e) {
                    break; // exit loop and thread
                  }
                }
              }
            }
        }
//        timer=null; GemStoneAddition - bad coding practice to null out variables like this
    }



    /* --------------------------------- Private Methods ----------------------------------- */


    /**
     * Sends ARE_YOU_DEAD message to suspected_mbr, wait for return or timeout
     */
    void suspect(SuspectMember sm) {  // GemStoneAddition - SuspectMember struct
        Message msg;
        if(sm == null) return;
        
        Address mbr = sm.suspectedMember;
        
        if (mbr.equals(this.local_addr)) { // GemStoneAddition - don't suspect self
          return;
        }
        
        synchronized(view_lock) {
          if (this.view == null) {
            return;
          }
        }
        // GemStoneAddition - try to use addresses from the view
        sm.suspectedMember = mbr = this.view.getMember(mbr);
        
        synchronized(suspects) {
            if(suspects.containsKey(sm)) {
              startTimer(); // GemStoneAddition - make sure "timer" is running
                return;
            }
            long suspectTime = System.currentTimeMillis();
            suspects.put(sm, Long.valueOf(suspectTime));
            suspectsAdded = true;
            suspects.notify();
        }
        if(trace) log.trace("verifying that " + mbr + " is gone");
        // GemStoneAddition - moved out of suspects sync to avoid deadlock in TP.send
        for(int i=0; i < num_msgs; i++) {
          msg=new Message(mbr, local_addr, null);
          msg.putHeader(name, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
          passDown(new Event(Event.MSG, msg));
        }
//        if(timer == null) GemStoneAddition, this check is already done in startTimer, and done correctly (synchronized)
        startTimer();
    }

    public void unsuspect(Address mbr) {
        if (mbr == null) return;
        if (((IpAddress)mbr).getBirthViewId() < 0) {  // GemStoneAddition - must have birth view ID
          if (log.getLogWriter().fineEnabled()) {
            log.getLogWriter().fine("Unsuspect() found view id missing from " + mbr + " which is abnormal if this member isn't in the process of joining");
          }
          return;
        }
        // GemStoneAddition - SuspectMember struct
        SuspectMember sm = new SuspectMember(local_addr, mbr);
        if (!suspects.containsKey(sm)) {
          return;
        }
        synchronized(suspects) {
            if (suspects.containsKey(sm)) {
              // GemStoneAddition - log the unsuspect if this is the membership coordinator
              if (this.view.getCreator().equals(this.local_addr)) {
                /*if(trace)*/ log.getLogWriter().info( ExternalStrings.VERIFY_SUSPECT_MEMBER_0_IS_NO_LONGER_SUSPECT, mbr);
              }
              suspects.remove(sm);
              passDown(new Event(Event.UNSUSPECT, mbr));
              passUp(new Event(Event.UNSUSPECT, mbr));
            }
        }
    }


    synchronized /* GemStoneAddition */ void startTimer() {
      /* GemStoneAddition - bug #42261: NPE during shutdown due to bad coding in this class.
       * Removed the nulling out of timer and added proper synchronized access
       */
      Thread tmp = timer;
        if(tmp == null || !tmp.isAlive()) {
            tmp=new Thread(GemFireTracer.GROUP, this, "VERIFY_SUSPECT.TimerThread");
            tmp.setDaemon(true);
            tmp.start();
            timer = tmp;
        }
    }

    @Override // GemStoneAddition  
    public synchronized /* GemStoneAddition */ void stop() {
        // GemStoneAddition - bug #42261: NPE during shutdown due to poor coding in this class
        Thread tmp = timer;
        if(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
        }
    }
    /* ----------------------------- End of Private Methods -------------------------------- */





    public static class VerifyHeader extends Header implements Streamable {
        static final short ARE_YOU_DEAD=1;  // 'from' is sender of verify msg
        static final short I_AM_NOT_DEAD=2;  // 'from' is suspected member

        short type=ARE_YOU_DEAD;
        Address from=null;     // member who wants to verify that suspected_mbr is dead


        public VerifyHeader() {
        } // used for externalization

        VerifyHeader(short type) {
            this.type=type;
        }

        VerifyHeader(short type, Address from) {
            this(type);
            this.from=from;
        }


        @Override // GemStoneAddition  
        public String toString() {
            switch(type) {
                case ARE_YOU_DEAD:
                    return "[VERIFY_SUSPECT: ARE_YOU_DEAD]";
                case I_AM_NOT_DEAD:
                    return "[VERIFY_SUSPECT: I_AM_NOT_DEAD]";
                default:
                    return "[VERIFY_SUSPECT: unknown type (" + type + ")]";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeShort(type);
            out.writeObject(from);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readShort();
            from=(Address)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(type);
            Util.writeAddress(from, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readShort();
            from=Util.readAddress(in);
        }

    }

}

