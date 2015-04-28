/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: MERGE2.java,v 1.25 2005/10/04 15:47:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;




/**
 * Protocol to discover subgroups; e.g., existing due to a network partition (that healed). Example: group
 * {p,q,r,s,t,u,v,w} is split into 3 subgroups {p,q}, {r,s,t,u} and {v,w}. This protocol will eventually send
 * a MERGE event with the coordinators of each subgroup up the stack: {p,r,v}. Note that - depending on the time
 * of subgroup discovery - there could also be 2 MERGE events, which first join 2 of the subgroups, and then the
 * resulting group to the last subgroup. The real work of merging the subgroups into one larger group is done
 * somewhere above this protocol (typically in the GMS protocol).<p>
 * This protocol works as follows:
 * <ul>
 * <li>If coordinator: periodically retrieve the initial membership (using the FIND_INITIAL_MBRS event provided e.g.
 *     by PING or TCPPING protocols. This list contains {coord,addr} pairs.
 * <li>If there is more than 1 coordinator:
 *     <ol>
 *     <li>Get all coordinators
 *     <li>Create a MERGE event with the list of coordinators as argument
 *     <li>Send the event up the stack
 *     </ol>
 * </ul>
 *
 * <p>
 *
 * Requires: FIND_INITIAL_MBRS event from below<br>
 * Provides: sends MERGE event with list of coordinators up the stack<br>
 * @author Bela Ban, Oct 16 2001
 */
public class MERGE2 extends Protocol  {
    Address       local_addr=null;
    FindSubgroups task=null;             // task periodically executing as long as we are coordinator
    private final Object task_lock=new Object();
    long          min_interval=5000;     // minimum time between executions of the FindSubgroups task
    long          max_interval=20000;    // maximum time between executions of the FindSubgroups task
    boolean       is_coord=false;
    final Promise find_promise=new Promise(); // to synchronize FindSubgroups.findInitialMembers() on

    /** Use a new thread to send the MERGE event up the stack */
    boolean       use_separate_thread=false;


    @Override // GemStoneAddition  
    public String getName() {
        return "MERGE2";
    }

    public long getMinInterval() {
        return min_interval;
    }

    public void setMinInterval(long i) {
        min_interval=i;
    }

    public long getMaxInterval() {
        return max_interval;
    }

    public void setMaxInterval(long l) {
        max_interval=l;
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("min_interval");
        if(str != null) {
            min_interval=Long.parseLong(str);
            props.remove("min_interval");
        }

        str=props.getProperty("max_interval");
        if(str != null) {
            max_interval=Long.parseLong(str);
            props.remove("max_interval");
        }

        if(min_interval <= 0 || max_interval <= 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.MERGE2_MIN_INTERVAL_AND_MAX_INTERVAL_HAVE_TO_BE__0);
            return false;
        }
        if(max_interval <= min_interval) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.MERGE2_MAX_INTERVAL_HAS_TO_BE_GREATER_THAN_MIN_INTERVAL);
            return false;
        }

        str=props.getProperty("use_separate_thread");
        if(str != null) {
            use_separate_thread=Boolean.valueOf(str).booleanValue();
            props.remove("use_separate_thread");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.MERGE2_MERGE2SETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public Vector requiredDownServices() {
        Vector retval=new Vector(1);
        retval.addElement(Integer.valueOf(Event.FIND_INITIAL_MBRS));
        return retval;
    }


    @Override // GemStoneAddition  
    public void stop() {
        is_coord=false;
        stopTask();
    }


    /**
     * This prevents the up-handler thread to be created, which is not needed in the protocol.
     * DON'T REMOVE ! 
     */
    @Override // GemStoneAddition  
    public void startUpHandler() {
    }


    /**
     * This prevents the down-handler thread to be created, which is not needed in the protocol.
     * DON'T REMOVE ! 
     */
    @Override // GemStoneAddition  
    public void startDownHandler() {
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                passUp(evt);
                break;

            case Event.FIND_INITIAL_MBRS_OK:
                find_promise.setResult(evt.getArg());
                passUp(evt); // could be needed by GMS
                break;

            default:
                passUp(evt);            // Pass up to the layer above us
                break;
        }
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Vector mbrs;
        Address coord;

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                passDown(evt);
                mbrs=((View)evt.getArg()).getMembers();
                if(mbrs == null || mbrs.size() == 0 || local_addr == null) {
                    stopTask();
                    break;
                }
                coord=(Address)mbrs.elementAt(0);
                if(coord.equals(local_addr)) {
                    is_coord=true;
                    startTask(); // start task if we became coordinator (doesn't start if already running)
                }
                else {
                    // if we were coordinator, but are no longer, stop task. this happens e.g. when we merge and someone
                    // else becomes the new coordinator of the merged group
                    if(is_coord) {
                        is_coord=false;
                    }
                    stopTask();
                }
                break;

            default:
                passDown(evt);          // Pass on to the layer below us
                break;
        }
    }


    /* -------------------------------------- Private Methods --------------------------------------- */
    void startTask() {
        synchronized(task_lock) {
            if(task == null)
                task=new FindSubgroups();
            task.start();
        }
    }

    void stopTask() {
        synchronized(task_lock) {
            if(task != null) {
                task.stop(); // will cause timer to remove task from execution schedule
                task=null;
            }
        }
    }
    /* ---------------------------------- End of Private Methods ------------------------------------ */




    /**
     * Task periodically executing (if role is coordinator). Gets the initial membership and determines
     * whether there are subgroups (multiple coordinators for the same group). If yes, it sends a MERGE event
     * with the list of the coordinators up the stack
     */
    protected/*GemStoneAddition*/ class FindSubgroups implements Runnable {
        // GemStoneAddition: #thread must be synchronized on this
        Thread thread=null;


        public synchronized /* GemStoneAddition */ void start() {
            if(thread == null || !thread.isAlive()) {
                thread=new Thread(this, "MERGE2.FindSubgroups thread");
                thread.setDaemon(true);
                thread.start();
            }
        }


        public synchronized /* GemStoneAddition */ void stop() {
            if(thread != null) {
                Thread tmp=thread;
                thread=null;
                tmp.interrupt(); // wakes up sleeping thread
                find_promise.reset();
            }
            thread=null;
        }


        public void run() {
            long interval;
            Vector coords;
            Vector initial_mbrs;

            // if(log.isDebugEnabled()) log.debug("merge task started as I'm the coordinator");
            for (;;) { // GemStoneAddition remove coding anti-pattern
                interval=computeInterval();
                try { // GemStoneAddition
                  Util.sleep(interval);
                }
                catch (InterruptedException e) {
                  break; // exits thread
                }
//                if(thread == null) break; GemStoneAddition
                if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                initial_mbrs=findInitialMembers();
//                if(thread == null) break; GemStoneAddition
                if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                if(log.isDebugEnabled()) log.debug("initial_mbrs=" + initial_mbrs);
                coords=detectMultipleCoordinators(initial_mbrs);
                if(coords != null && coords.size() > 1) {
                    if(log.isDebugEnabled())
                        log.debug("found multiple coordinators: " + coords + "; sending up MERGE event");
                    final Event evt=new Event(Event.MERGE, coords);
                    if(use_separate_thread) {
                        Thread merge_notifier=new Thread() {
                          @Override // GemStoneAddition  
                            public void run() {
                                passUp(evt);
                            }
                        };
                        merge_notifier.setDaemon(true);
                        merge_notifier.setName("merge notifier thread");
                        merge_notifier.start();
                    }
                    else {
                        passUp(evt);
                    }
                }
            }
            if(trace)
                log.trace("MERGE2.FindSubgroups thread terminated (local_addr=" + local_addr + ")");
        }


        /**
         * Returns a random value within [min_interval - max_interval]
         */
        long computeInterval() {
            return min_interval + Util.random(max_interval - min_interval);
        }


        /**
         * Returns a list of PingRsp pairs.
         */
        Vector findInitialMembers() {
            PingRsp tmp=new PingRsp(local_addr, local_addr, true);
            find_promise.reset();
            passDown(Event.FIND_INITIAL_MBRS_EVT);
            Vector retval=(Vector)find_promise.getResult(0); // wait indefinitely until response is received
            if(retval != null && is_coord && local_addr != null && !retval.contains(tmp))
                retval.add(tmp);
            return retval;
        }


        /**
         * Finds out if there is more than 1 coordinator in the initial_mbrs vector (contains PingRsp elements).
         * @param initial_mbrs A list of PingRsp pairs
         * @return Vector A list of the coordinators (Addresses) found. Will contain just 1 element for a correct
         *         membership, and more than 1 for multiple coordinators
         */
        Vector detectMultipleCoordinators(Vector initial_mbrs) {
            Vector ret=new Vector(11);
            PingRsp rsp;
            Address coord;

            if(initial_mbrs == null) return null;
            for(int i=0; i < initial_mbrs.size(); i++) {
                rsp=(PingRsp)initial_mbrs.elementAt(i);
                if(!rsp.is_server)
                    continue;
                coord=rsp.getCoordAddress();
                if(!ret.contains(coord))
                    ret.addElement(coord);
            }

            return ret;
        }

    }

}
