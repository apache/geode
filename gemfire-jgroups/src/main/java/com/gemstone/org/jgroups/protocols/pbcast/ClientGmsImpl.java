/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ClientGmsImpl.java,v 1.29 2005/11/22 11:58:28 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.ShunnedAddressException;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.PingRsp;
import com.gemstone.org.jgroups.protocols.UNICAST;
import com.gemstone.org.jgroups.protocols.UNICAST.UnicastHeader;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.Util;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;


/**
 * Client part of GMS. Whenever a new member wants to join a group, it starts in the CLIENT role.
 * No multicasts to the group will be received and processed until the member has been joined and
 * turned into a SERVER (either coordinator or participant, mostly just participant). This class
 * only implements <code>Join</code> (called by clients who want to join a certain group, and
 * <code>ViewChange</code> which is called by the coordinator that was contacted by this client, to
 * tell the client what its initial membership is.
 * @author Bela Ban
 * @version $Revision: 1.29 $
 */
public class ClientGmsImpl extends GmsImpl  {
    public static volatile boolean SLOW_JOIN; // test hook for bug #49897
    public static volatile Object SLOW_JOIN_LOCK;
    
    private final Vector  initial_mbrs=new Vector(11);
    private volatile/*GemStoneAddition*/ boolean       initial_mbrs_received=false; // GemStoneAddition - accesses synchronized on initial_mbrs
    private final Promise join_promise=new Promise();

    private volatile JoinRsp getViewResponse; // GemStoneAddition - when becoming coord of existing group
    private volatile int getViewResponses;
    private final Object getViewLock = new Object();
    private Set missing_mbrs = null;
    
    private int findInitialMbrsAttempts = 0;


    public ClientGmsImpl(GMS g) {
        super(g);
    }

    @Override // GemStoneAddition
    public void init() throws Exception {
        super.init();
        synchronized(initial_mbrs) {
            initial_mbrs.clear();
            initial_mbrs_received=false;
        }
        join_promise.reset();
    }


    /**
     * Joins this process to a group. Determines the coordinator and sends a unicast
     * handleJoin() message to it. The coordinator returns a JoinRsp and then broadcasts the new view, which
     * contains a message digest and the current membership (including the joiner). The joiner is then
     * supposed to install the new view and the digest and starts accepting mcast messages. Previous
     * mcast messages were discarded (this is done in PBCAST).<p>
     * If successful, impl is changed to an instance of ParticipantGmsImpl.
     * Otherwise, we continue trying to send join() messages to	the coordinator,
     * until we succeed (or there is no member in the group. In this case, we create our own singleton group).
     * <p>When GMS.disable_initial_coord is set to true, then we won't become coordinator on receiving an initial
     * membership of 0, but instead will retry (forever) until we get an initial membership of > 0.
     * @param myAddr Our own address (assigned through SET_LOCAL_ADDRESS)
     */
    @Override // GemStoneAddition
    public boolean join(Address myAddr) {
        Address coord = null;
        Address coordSentJoin = null;
        JoinRsp rsp;
        Digest tmp_digest;
        View tmp_view = null;
        leaving=false;
        long starttime = System.currentTimeMillis(); // GemStoneAddition
        final int maxRetries = 6; // GemStoneAddition - need this figure later
        int joinRetries = maxRetries;

        join_promise.reset();
        boolean debugFailFirst = Boolean.getBoolean("p2p.DEBUG_FAIL_FIRST");
        while(!leaving) {
            joinRetries--;
            if (joinRetries < 0) {
              // GemStoneAddition
              // if not just anyone can be the coordinator and the elected \
              // coordinator cannot process a join_req, assume it is
              // gone and elect self as new coordinator
              if (coord != null && gms.floatingCoordinatorDisabled
                  && !gms.splitBrainDetectionEnabled // bug #41772  I DON'T THINK THIS IS TRUE WITH QUORUM
                  && myAddr.preferredForCoordinator()) {
                Address failedCoord = coord;
                if (!myAddr.equals(failedCoord)) {
                  gms.log.getLogWriter().warning(
                    ExternalStrings.ClientGmsImpl_COULD_NOT_JOIN_DISTRIBUTED_SYSTEM_0,
                    failedCoord);
                  if (becomeGroupCoordinator(myAddr, failedCoord)) {
                    return true;
                  }
                }
                coord = null;
              }
              else {
                if (!join_promise.hasResult()) {  // GemStoneAddition - check one more time since we just waited for the retry
                  log.warn("Could not join distributed system using member " + coord
                      + ".  gms.floatingCoordinatorDisabled="+gms.floatingCoordinatorDisabled
                      + ", mbr.canBeCoordinator()=" + myAddr.preferredForCoordinator());
                  return false;
                }
              }
            }
 
            //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "findInitialMembers");
            missing_mbrs = null;
            findInitialMembers();
            //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "findInitialMembers done");
            if (debugFailFirst) {
              log.getLogWriter().severe(ExternalStrings.ClientGmsImpl_IGNORING_FIRST_MEMBERSHIP_SET_FOR_DEBUGGING);
               debugFailFirst = false;
               continue;
            }
            if (!join_promise.hasResult()) {
              if(log.isDebugEnabled()) log.debug("CGMS: initial_mbrs are " + initial_mbrs + "; missing_mbrs are " + missing_mbrs);
              if(initial_mbrs.size() == 0) {
                if (missing_mbrs == null || missing_mbrs.isEmpty()) { // GemStoneAddition
                  if(gms.disable_initial_coord) {
                    // GemStoneAddition - there was no timeout if disable_ic was set!!
                    checkForTimeout(starttime);
                    //if(trace)
                    log.warn("received an initial membership of 0, but cannot become coordinator " + // GemStoneAddition - was trace level output.  Debug #34274
                    "(disable_initial_coord=true), will retry fetching the initial membership");
                    //                    continue;
                  }
                  // if we're getting no responses at all after two or more attempts, there's no-one
                  // out there.
                  else if (joinRetries <= (maxRetries - 2)) {
                    if(log.isDebugEnabled())
                      log.debug("no initial members discovered: creating group as first member");
                    becomeSingletonMember(myAddr);
                    return true;
                  }
                }
                if (joinRetries > 0) {
                  try { // GemStoneAddition
                    //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #1");
                    Util.sleep(gms.join_retry_timeout);
                    //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #1 done");
                  } catch (InterruptedException e) {
                    if(log.isDebugEnabled())
                      log.debug("interrupted; creating group as first member");
                    becomeSingletonMember(myAddr);
                    Thread.currentThread().interrupt(); // GemStoneAddition
                    return true;
                  }
                }
                continue;
              }

              //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "determining coordinator");
              coord=determineCoord(initial_mbrs);
              //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "done determining coordinator: " + coord);
              // GemStoneAddition - don't elect a new coordinator on the first attempt
              if(coord == null) {
                if (joinRetries < maxRetries-1) {
                  if(gms.handle_concurrent_startup == false) {
                    if(trace)
                      log.trace("handle_concurrent_startup is false; ignoring responses of initial clients");
                    becomeSingletonMember(myAddr);
                    return true;
                  }

                  if(trace)
                    log.trace("could not determine coordinator from responses " + initial_mbrs);

                  // the member to become singleton member (and thus coord) is the first of all clients
                  Set clients=new TreeSet(); // sorted
                  clients.add(myAddr); // add myself again (was removed by findInitialMembers())
                  for(int i=0; i < initial_mbrs.size(); i++) {
                    PingRsp pingRsp=(PingRsp)initial_mbrs.elementAt(i);
                    Address client_addr=pingRsp.getCoordAddress(); // GemStoneAddition - was .getAddress();
                    if(client_addr != null)
                      clients.add(client_addr);
                  }
                  if(trace)
                    log.trace("Choosing coordinator from these servers: " + clients + ".\nJoin retries remaining: " + joinRetries);
                  Address new_coord=(Address)clients.iterator().next();
                  if (new_coord.equals(myAddr)) {
                    if (!gms.disable_initial_coord) { // GemStoneAddition - respect disable_initial_coord
                      if(trace)
                        log.trace("I (" + myAddr + ") am the first of the clients, and will become coordinator");
                      becomeSingletonMember(myAddr);
                      // GemStoneAddition - tell the DM to wait a while
                      //if (gms.stack.jgmm != null) {
                      //  long waitPeriod = Long.getLong("p2p.concStartupDelay", gms.join_timeout).longValue();
                      //  if (trace)
                      //    log.trace("Telling the manager to wait for " + waitPeriod + " ms");
                      //  gms.stack.jgmm.establishChannelPause(waitPeriod);
                      //  //try { Thread.sleep(waitPeriod); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                      //}
                      return true;
                    }
                  }
                  else {
//                    if(trace)
//                      log.trace("I (" + myAddr + ") am not the first of the clients, " +
//                      "waiting for another client to become coordinator");
                    log.getLogWriter()
                      .info(ExternalStrings.DEBUG, "Waiting for " + new_coord
                          + " to possibly become coordinator during concurrent startup.  "
                          + "My address is " + gms.local_addr);
                    // GemStoneAddition - if there is concurrent startup, the other
                    // client will be waiting the same amount of time, so increase the
                    // join attempts in this VM to let the other client finish
                    if (joinRetries < 2) {
                      checkForTimeout(starttime);
                      joinRetries++;
                    }
                  }
                } // joinRetries < maxRetries-1
                else {
                  if (trace)
                    log.trace("I (" + myAddr + ") am not the coordinator and it is too" +
                    " soon for an election");
                }
                if (joinRetries > 0) {
                  try { // GemStoneAddition
                    //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #2");
                    Util.sleep(gms.join_retry_timeout);
                    //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #2 done");
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false; // treat as failure
                  }
                }

                continue;
              } // coord==null 

              if (!coord.splitBrainEnabled() && this.gms.splitBrainDetectionEnabled) {
                throw gms.stack.gfBasicFunctions.getGemFireConfigException(
                    "Group membership coordinator, " + coord + " does not have " +
                "network partition detection enabled, but this DistributedSystem has it enabled");
              }

              boolean c6 = (((IpAddress)coord).getIpAddress() instanceof Inet6Address);
              boolean m6 = (((IpAddress)myAddr).getIpAddress() instanceof Inet6Address);
              if (c6 && !m6) {
                throw gms.stack.gfBasicFunctions.getGemFireConfigException(
                    "Group membership coordinator, " + coord + ", is using IPv6 but this "
                    + "process is using IPv4");
              }
              if (m6 && !c6) {
                throw gms.stack.gfBasicFunctions.getGemFireConfigException(
                    "Group membership coordinator, " + coord + ", is using IPv4 but this "
                    + "process is using IPv6");
              }
            } // !join_promise.hasResult()
            else {
              coord = coordSentJoin;
            }
            
            if (coord == null) {
              // GemStoneAddition
              // fix for NPE when a false join response has been received.  This could
              // be from the retransmission of a response sent to a previous member
              // using the same address.  In this case we haven't even gotten to
              // the point of sending a join request and the response should be ignored
              join_promise.reset();
              continue;
            }

            try {
              if (coord.equals(gms.local_addr) && gms.local_addr.preferredForCoordinator()) {
                // GemStoneAddition - we can't usurp a group that was using a coordinator
                // that had the same address that this process is now using (bug #41772)
                if (((IpAddress)coord).getUniqueID() != ((IpAddress)gms.local_addr).getUniqueID()) {
                  throw new ShunnedAddressException();
                }
                // this member was elected by others as the coordinator
                if (becomeGroupCoordinator(gms.local_addr, null)) {
                  return true;
                }
                else {
                  coord = null;
                  continue; // no need to sleep - there's another coordinator now
                }
              }
              // GemStoneAddition - check to see if a result came in while get_mbrs was being processed.
              // If there is one, just use it.
              if (!join_promise.hasResult()) {
                  if(log.isDebugEnabled())
                      log.debug("sending join_req(" + myAddr + ") to " + coord);
                  coordSentJoin = coord;
                  sendJoinMessage(coord, myAddr);
              }
              if (log.isDebugEnabled()) {
                log.debug("Waiting for join response");
              }
              
              // test hook for bug #49897
              if (SLOW_JOIN) {
                synchronized(SLOW_JOIN_LOCK) {
                  try {
                    SLOW_JOIN_LOCK.wait();
                  } catch (InterruptedException e) {
                    // the test will interrupt.  Set the interrupt flag
                    // so the Promise.getResult() call will run into it
                    Thread.currentThread().interrupt();
                  }
                }
              }
                rsp=(JoinRsp)join_promise.getResult(gms.join_timeout);

                // bug #49897 - don't keep going if the thread's been interrupted
                if (Thread.currentThread().isInterrupted()) {
                  throw gms.stack.gfBasicFunctions.getSystemConnectException(ExternalStrings.ClientGmsImpl_JOIN_INTERRUPTED.toLocalizedString());
                }

                if(rsp == null) {
                    if(warn && !leaving /* GemStoneAddition */) log.getLogWriter().warning(ExternalStrings.ClientGmsImpl_JOIN_0__SENT_TO__1__TIMED_OUT_RETRYING, new Object[] {myAddr, coord});
                }
                else {
                  if (log.isDebugEnabled()) {
                    log.debug("Got join response: " + rsp);
                  }
                  // GemStoneAddition - birth view ID
                  if (rsp.view != null && rsp.view.getVid() != null) {
                    ((IpAddress)gms.local_addr).setBirthViewId(rsp.view.getVid().getId());
                  }
                    if(rsp.getFailReason() != null){
                      if (rsp.getFailReason().equals(JoinRsp.SHUNNED_ADDRESS)) {
                        throw new ShunnedAddressException();
                      }
                      if (rsp.getFailReason().contains("Rejecting the attempt of a member using an older version")
                          || rsp.getFailReason().contains("15806")) {
                        throw gms.stack.gfBasicFunctions.getSystemConnectException(rsp.getFailReason());
                      }
                      throw gms.stack.gfBasicFunctions.getAuthenticationFailedException(rsp.getFailReason());
                    }
                    coord = rsp.getView().getCreator();
                    gms.notifyOfCoordinator(coord);
                    // 1. Install digest
                    tmp_digest=rsp.getDigest();
                    tmp_view=rsp.getView();
                    if(tmp_digest == null || tmp_view == null) {
                      if(log.isErrorEnabled()) log.error(ExternalStrings.ClientGmsImpl_DIGEST_OR_VIEW_OF_JOIN_RESPONSE_IS_NULL);
                    }
                    else {
                      // GemStoneAddition - commented out inc of highSeqno because this
                      // does not work if the view is not multicast.  In view of this there
                      // must be a retransmission protocol.
//                        tmp_digest.incrementHighSeqno(coord); 	// see DESIGN for an explanantion
                        if(log.isDebugEnabled()) log.debug("digest is " + tmp_digest);
                        gms.setDigest(tmp_digest);

                        // 2. Install view
                        if(log.isDebugEnabled()) log.debug("[" + gms.local_addr + "]: JoinRsp=" + rsp.getView() +
                                " [size=" + rsp.getView().size() + "]\n\n");
    
                        if(!installView(tmp_view)) {
                            if(log.isErrorEnabled()) log.error(ExternalStrings.ClientGmsImpl_VIEW_INSTALLATION_FAILED_RETRYING_TO_JOIN_GROUP);
                            continue;
                        }

                        // send VIEW_ACK to sender of view
                        Message view_ack=new Message(coord, null, null);
                        GMS.GmsHeader tmphdr=new GMS.GmsHeader(GMS.GmsHeader.VIEW_ACK);
                        view_ack.putHeader(GMS.name, tmphdr);
                        // GemStoneAddition: bug #49261 - do not send the view
                        // so that the ack-collector will accept this as an ack
                        // for the view about to be cast by the coordinator
                        view_ack.setObject(null); 
                        gms.passDown(new Event(Event.MSG, view_ack));

                        gms.passUp(new Event(Event.BECOME_SERVER));
                        gms.passDown(new Event(Event.BECOME_SERVER));
                        return true;
                    }
                }
            }
            catch (ShunnedAddressException e) {
              System.setProperty("gemfire.jg-bind-port", "0"); // for testing
              throw e;
            }
            catch (RuntimeException e) {
              String name = e.getClass().getSimpleName();
              if (name.equals("AuthenticationFailedException")) {
                if (log.isDebugEnabled()) {
                  log.debug("AuthenticationFailedException=" + e.toString() );
                }
                throw e;
              } else if (name.equals("SystemConnectException")) {
                throw e;
              }
              if (log.getLogWriter().infoEnabled()) {
                log.getLogWriter().info(ExternalStrings.UnexpectedException, e);
              }
            }
            catch (Exception e) {
                if (log.getLogWriter().infoEnabled()) {
                  log.getLogWriter().info(ExternalStrings.UnexpectedException, e);
                }
            }

            if (joinRetries > 0) {
              try { // GemStoneAddition
                //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #3");
                Util.sleep(gms.join_retry_timeout);
                //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join sleep #3 done");
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false; // treat as failure
              }
            }
        }
        if (missing_mbrs != null && missing_mbrs.size() > 0) {
          log.getLogWriter().info(ExternalStrings.DEBUG, "missing ping responses from " + missing_mbrs);
          return false;
        }
        return true;
    }


    // GemStoneAddition: check to see if join attempts have timed out
    private void checkForTimeout(long starttime) {
      if ((starttime + (2 * gms.join_timeout)) < System.currentTimeMillis()) {
        if (!gms.stack.getChannel().closing()) {
          log.getLogWriter().fine("GMS client is timing out and forcing an exit");
        }
        throw gms.stack.gfBasicFunctions.getGemFireConfigException("Unable to contact a Locator service.  " +
        "Operation either timed out, was stopped or Locator does not exist.");
      }
    }

    @Override // GemStoneAddition
    public void leave(Address mbr) {
        leaving=true;
        wrongMethod("leave");
    }


    @Override // GemStoneAddition
    public void handleJoinResponse(JoinRsp join_rsp) {
      if (log.isDebugEnabled()) {
        log.getLogWriter().info(ExternalStrings.DEBUG, "Received join response " + join_rsp);
      }
      while (SLOW_JOIN) { // test hook
        log.getLogWriter().info(ExternalStrings.DEBUG, "delaying delivery of join response");
        try { Thread.sleep(5000); } catch (InterruptedException e) { return; }
      }
        join_promise.setResult(join_rsp); // will wake up join() method
        //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "Set join response in promise");
    }

    @Override // GemStoneAddition
    public void handleLeaveResponse(String reason) { // GemStoneAddition - added reason
    }

    // GemStoneAddition - optimized view casting
    @Override
    public void handleJoinsAndLeaves(List joins, List leaves, List suspects, List suspectReasons, boolean forceInclusion) {}

//    @Override // GemStoneAddition
//    public void suspect(Address mbr) {
//    }

    @Override // GemStoneAddition
    public void unsuspect(Address mbr) {
    }


    @Override // GemStoneAddition
    public void handleJoin(Address mbr) {
    }
    
//    @Override // GemStoneAddition
//    public void handleJoin(List mbrList) {  // GemStoneAddition
//    }


    /** Returns false. Clients don't handle leave() requests */
    public void handleLeave(Address mbr, boolean suspected) {
    }
    
    // GemStoneAddition
    @Override // GemStoneAddition
    public void handleLeave(List memberList, boolean suspected, List reasons, boolean forceInclusion) {
    }


    /**
     * GemStoneAddition - handle get_view_rsp when becoming
     * coordinator of existing group
     */
    @Override // GemStoneAddition
    public synchronized void handleGetViewResponse(JoinRsp response) {
      synchronized (this.getViewLock) {
        if (this.getViewResponse != null) {
          if (this.getViewResponse.getView().getCreator().equals(response.getView().getCreator()) &&
              this.getViewResponse.getView().getVid().compareTo(response.getView().getVid()) < 0) {
              // newer view - use it
            this.getViewResponses++;
            this.getViewResponse = response;
          }
        }
        else {
          this.getViewResponse = response;
          this.getViewResponses++;
        }
        this.getViewLock.notify();
      }
    }
    
    @Override // GemStoneAddition
    public void handleViewChange(View new_view, Digest digest) {
    }


    /**
     * Called by join(). Installs the view returned by calling Coord.handleJoin() and
     * becomes coordinator.
     */
    private boolean installView(View new_view) {
        Vector mems=new_view.getMembers();
         if(log.isDebugEnabled()) log.debug("new_view=" + new_view);
        if(gms.local_addr == null || mems == null || !mems.contains(gms.local_addr)) {
            if(log.isErrorEnabled()) log.error("I (" + gms.local_addr +
                                                       ") am not member of " + mems + ", will not install view");
            return false;
        }
        gms.installView(new_view);
        gms.becomeParticipant();
        gms.passUp(new Event(Event.BECOME_SERVER));
        gms.passDown(new Event(Event.BECOME_SERVER));
        return true;
    }


    /** Returns immediately. Clients don't handle suspect() requests */
//    @Override // GemStoneAddition
//    public void handleSuspect(Address mbr) {
//    }


    @Override // GemStoneAddition
    public boolean handleUpEvent(Event evt) {
        Vector tmp;

        switch(evt.getType()) {

            case Event.FIND_INITIAL_MBRS_OK:
                tmp=(Vector)evt.getArg();
                synchronized(initial_mbrs) {
                  if (!initial_mbrs_received) { // GemStoneAddition - don't update if thread was interrupted
                    if(tmp != null && tmp.size() > 0) {
                        initial_mbrs.addAll(tmp);
                    }
                    initial_mbrs_received=true;
//if (initial_mbrs.size() == 0) {
//  log.getLogWriter().warning("received FIND_INITIAL_MBRS_OK event with no members", new Exception("Stack trace"));
//}
                    initial_mbrs.notifyAll();
                  }
                }
                return false;  // don't pass up the stack
                
            case Event.FIND_INITIAL_MBRS_FAILED: // GemStoneAddition - requiredMembers.  See bug #30341
              if (log.getLogWriter().fineEnabled()) {
                log.getLogWriter().fine("received FIND_INITIAL_MBRS_FAILED event in ClientGmsImpl with " + evt.getArg());
              }
              Set missing = (Set)evt.getArg();
              synchronized(initial_mbrs) {
                if (!initial_mbrs_received) {
                  initial_mbrs_received = true;
                  missing_mbrs = missing;
                  initial_mbrs.notifyAll();
                }
              }
              return false;

            case Event.EXIT: // GemStoneAddition - stop looking if we exit
                synchronized(initial_mbrs) {
                  initial_mbrs_received=true;
                  initial_mbrs.notifyAll();
                }
                stop();
                break;
            
        }
        return true;
    }





    /* --------------------------- Private Methods ------------------------------------ */


    // GemStoneAddition - lastAttempt is used to prevent sending join to the
    // same recipient multiple times.  The UNICAST protocol handles retransmission
    Address lastAttempt;
    
    void sendJoinMessage(Address coord, Address mbr) {
        Message msg;
        GMS.GmsHeader hdr;

        // GemStoneAddition:
        // if reconnecting we will make multiple attempts with the same coordinator
        // because it might be spinning up at the same time.  Otherwise, if not reconnecting,
        // we don't want to keep trying with the same coordinator because if it's not responding
        // it's probably not there
        if (lastAttempt == null
            || (this.gms.stack.gfPeerFunctions.isReconnectingDS()
                || !coord.equals(lastAttempt))) {
          log.getLogWriter().info(ExternalStrings.
              ClientGmsImpl_ATTEMPTING_TO_JOIN_DS_WHOSE_MEMBERSHIP_COORDINATOR_IS_0_USING_ID_1,
          new Object[]{coord, mbr});
          msg=new Message(coord, null, null);
          hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, mbr);
          msg.putHeader(gms.getName(), hdr);
          lastAttempt = null; // set to null in case there is an exception in passDown()
          gms.passDown(new Event(Event.MSG, msg));
          lastAttempt = coord;
        }
    }


    /**
     * Pings initial members. Removes self before returning vector of initial members.
     * Uses IP multicast or gossiping, depending on parameters.
     */
    void findInitialMembers() {
        PingRsp ping_rsp;

        synchronized(initial_mbrs) {
            findInitialMbrsAttempts++; // GemStoneAddition
            
            initial_mbrs.removeAllElements();
            initial_mbrs_received=false;
            gms.passDown(new Event(Event.FIND_INITIAL_MBRS));

            // the initial_mbrs_received flag is needed when passDown() is executed on the same thread, so when
            // it returns, a response might actually have been received (even though the initial_mbrs might still be empty)
            while/*GemStoneAddition*/ (initial_mbrs_received == false) {
                try {
                    initial_mbrs.wait(); //gms.join_timeout+1000); // GemStoneAddition bug #34274 - don't wait forever
                }
                catch(InterruptedException e) { // GemStoneAddition
                  Thread.currentThread().interrupt();
                  initial_mbrs_received = true; // suppress any late updates
                  // process whatever he have now.  Note that since we are
                  // stilled synchronized on initial_mbrs, asynchronous updates
                  // won't come in before we return a result.
                }
            }

            for(int i=0; i < initial_mbrs.size(); i++) {
                ping_rsp=(PingRsp)initial_mbrs.elementAt(i);
                if(ping_rsp.own_addr != null && gms.local_addr != null &&
                        ping_rsp.own_addr.equals(gms.local_addr)) {
                    initial_mbrs.removeElementAt(i);
                    break;
                }
            }
            if (findInitialMbrsAttempts > 2) {
              // GemStoneAddition: bug #50785 - if we have made some rounds using
              // best-guess coordinator candidates from other processes that have
              // yet to join the group then we stop trusting this gossip
              // because it can just ping-pong around between members that are
              // still trying to join.  See PingWaiter.getPossibleCoordinator().
              List<PingRsp> removals = new ArrayList<PingRsp>(initial_mbrs.size());
              for(int i=0; i < initial_mbrs.size(); i++) {
                ping_rsp=(PingRsp)initial_mbrs.elementAt(i);
                if(ping_rsp.own_addr != null && !ping_rsp.isServer()
                    && !ping_rsp.getCoordAddress().equals(ping_rsp.own_addr)) {
                  removals.add(ping_rsp);
                }
              }
              initial_mbrs.removeAll(removals);
            }
        }
    }


    /**
     The coordinator is determined by a majority vote. If there are an equal number of votes for
     more than 1 candidate, we determine the winner randomly.
     */
    private Address determineCoord(Vector mbrs) {
        PingRsp mbr;
        Hashtable votes;
        int count, most_votes;
        Address winner=null, tmp;

        if(mbrs == null || mbrs.size() < 1)
            return null;

        votes=new Hashtable(5);

        // count *all* the votes (unlike the 2000 election)
        for(int i=0; i < mbrs.size(); i++) {
            mbr=(PingRsp)mbrs.elementAt(i);
            if(mbr.is_server && mbr.coord_addr != null) {
                if(!votes.containsKey(mbr.coord_addr))
                    votes.put(mbr.coord_addr, Integer.valueOf(1));
                else {
                    count=((Integer)votes.get(mbr.coord_addr)).intValue();
                    votes.put(mbr.coord_addr, Integer.valueOf(count + 1));
                }
            }
        }

        if(votes.size() > 1) {
            /*if(warn)*/ log.getLogWriter().warning(ExternalStrings.ClientGmsImpl_THERE_WAS_MORE_THAN_1_CANDIDATE_FOR_COORDINATOR__0, votes);
        }
        else {
            if(log.isDebugEnabled()) log.debug("election results: " + votes);
        }

        // determine who got the most votes
        most_votes=0;
        for(Enumeration e=votes.keys(); e.hasMoreElements();) {
            tmp=(Address)e.nextElement();
            count=((Integer)votes.get(tmp)).intValue();
            if(count > most_votes) {
                winner=tmp;
                // fixed July 15 2003 (patch submitted by Darren Hobbs, patch-id=771418)
                most_votes=count;
            }
        }
        votes.clear();
        return winner;
    }


    void becomeSingletonMember(Address mbr) {
        Digest initial_digest;
        ViewId view_id;
        Vector mbrs=new Vector(1);

        // set the initial digest (since I'm the first member)
        initial_digest=new Digest(1);             // 1 member (it's only me)
        initial_digest.add(gms.local_addr, 0, 0); // initial seqno mcast by me will be 1 (highest seen +1)
        gms.setDigest(initial_digest);

        view_id=new ViewId(mbr);       // create singleton view with mbr as only member
        mbrs.addElement(mbr);
        gms.installView(new View(view_id, mbrs));
        gms.becomeCoordinator(null); // not really necessary - installView() should do it

        gms.passUp(new Event(Event.BECOME_SERVER));
        gms.passDown(new Event(Event.BECOME_SERVER));

        gms.notifyOfCoordinator(gms.local_addr);

//log.getLogWriter().warning("Created new group with view " + gms.view_id);
        if(log.isDebugEnabled()) log.debug("created group (first member). My view is " + gms.view_id +
                                           ", impl is " + gms.getImpl().getClass().getName());
    }

    
    /**
     * GemStoneAddition - new protocol to allow a member to assume the role
     * of coordinator in a system that has lost all eligible coordinators
     * @param mbr the address of this stack
     * @param failedCoordinator (optional) the address of the coordinator that held the view
     * @return true if able to become coordinator, false if not
     */
    boolean becomeGroupCoordinator(Address mbr, Address failedCoordinator) {
      int sentRequests = 0;
      Digest initial_digest;
      ViewId view_id;
      Vector mbrs;
      Vector imbrs;
      synchronized(initial_mbrs) {
        imbrs = (Vector)initial_mbrs.clone();
      }
      
      // get the current view from one of the members and then
      // form a new group
      GMS.GmsHeader hdr = new GMS.GmsHeader(GMS.GmsHeader.GET_VIEW);
      for (int i=0; i<imbrs.size(); i++) {
        PingRsp rsp = (PingRsp)imbrs.get(i);
        Message getView = new Message();
        getView.setDest(rsp.getAddress());
        getView.putHeader(GMS.name, hdr);
        gms.passDown(new Event(Event.MSG, getView));
      }
      
      // wait up to ack-collection-time for responses
      sentRequests = imbrs.size();
      long endTime = System.currentTimeMillis() + gms.view_ack_collection_timeout;
      while (true) {
        synchronized (this.getViewLock) {
          long timeLeft = endTime - System.currentTimeMillis();
          if (this.getViewResponses >= sentRequests || timeLeft <= 0) {
            break;
          }
          try {
            this.getViewLock.wait(timeLeft);
          }
          catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw gms.stack.gfBasicFunctions.getSystemConnectException(ExternalStrings.ClientGmsImpl_INTERRUPTED_WHILE_BECOMING_COORDINATOR_OF_EXISTING_GROUP.toLocalizedString());
          }
        }
      }

      if (this.getViewResponse == null) {
        throw gms.stack.gfBasicFunctions.getSystemConnectException(ExternalStrings.ClientGmsImpl_UNABLE_TO_BECOME_COORDINATOR_OF_EXISTING_GROUP_BECAUSE_NO_VIEW_RESPONSES_WERE_RECEIVED.toLocalizedString());
      }
      
      View theView = this.getViewResponse.getView();
      
      Address theCreator = theView.getCreator();
      
      if (theCreator != null && theCreator.preferredForCoordinator()) {
        if (failedCoordinator == null && theView.containsMember(theCreator)) {
          // locator was shut down and restarted.  Other members reported no coordinator
          // but now there is one, so two eligible coordinators are being started
          // simultaneously.  Return false to allow the other one to win
//          log.getLogWriterI18n().info(JGroupsStrings.ONE_ARG, "DEBUG: failedCoordinator is null and view creator is "
//            + theCreator);
          return false;
        }
        
        if (failedCoordinator != null && !theCreator.equals(failedCoordinator)) {
          // the locator or its machine failed and this is a new locator starting
          // up.  Another locator has already installed itself as the coordinator,
          // so return false to let it win
//          log.getLogWriterI18n().info(JGroupsStrings.ONE_ARG, "DEBUG: failedCoordinator is " + failedCoordinator + " and view creator is "
//            + theCreator);
          return false;
        }
      }

      mbrs = theView.getMembers();
      mbrs.add(mbr); // add myself in  (was .add(0, mbr) prior to 2013 cedar release but this caused elder init problems with colocated secure locators)

      // create the initial digest.  it will be filled in with gossip soon enough
      initial_digest=this.getViewResponse.getDigest();
      initial_digest.add(gms.local_addr, 0, 0); // initial seqno mcast by me will be 1 (highest seen +1)
      gms.setDigest(initial_digest);

      view_id=new ViewId(mbr, theView.getVid().getId()+1);       // create singleton view with mbr as only member
      View new_view = new View(view_id, mbrs);
      gms.installView(new_view);
      gms.becomeCoordinator(null); // not really necessary - installView() should do it

      gms.ucastViewChange(new_view); // tell the world

      gms.passUp(new Event(Event.BECOME_SERVER));
      gms.passDown(new Event(Event.BECOME_SERVER));
      
      gms.notifyOfCoordinator(gms.local_addr);

      log.getLogWriter().info(
        ExternalStrings. ClientGmsImpl_RECREATED_DISTRIBUTED_SYSTEM_GROUP_0,
        gms.view);
      return true;
    }
    
//    @Override // GemStoneAddition
//    public void suspect(Address mbr, String reason) {
//      suspect(mbr);
//    }

    /* (non-Javadoc) GemStoneAddition
     * @see com.gemstone.org.jgroups.protocols.pbcast.GmsImpl#handleAlreadyJoined(com.gemstone.org.jgroups.Address)
     */
    @Override // GemStoneAddition
    public void handleAlreadyJoined(Address mbr) {
    }

}
