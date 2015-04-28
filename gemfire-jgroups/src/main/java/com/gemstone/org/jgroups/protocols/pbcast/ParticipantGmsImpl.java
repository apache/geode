/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ParticipantGmsImpl.java,v 1.17 2005/12/23 14:57:06 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;


public class ParticipantGmsImpl extends GmsImpl  {
    private final Vector     suspected_mbrs=new Vector(11);
    private final Set<Address> departed_mbrs = new HashSet<Address>(); // GemStoneAddition
    private final Promise    leave_promise=new Promise();


    public ParticipantGmsImpl(GMS g) {
        super(g);
    }


    @Override // GemStoneAddition
    public void init() throws Exception {
        super.init();
        synchronized(suspected_mbrs) { // GemStoneAddition
          suspected_mbrs.removeAllElements();
        }
        leave_promise.reset();
    }

    @Override // GemStoneAddition
    public boolean join(Address mbr) { // GemStoneAddition - return boolean
        wrongMethod("join");
        return false;
    }


    @Override
    public void handleJoinsAndLeaves(List joins, List leaves, List suspects, List suspectReasons, boolean forceInclusion) {
      for (Iterator it=suspects.iterator(); it.hasNext(); ) {
        handleSuspect((Address)it.next());
      }
      if (leaves != null && !leaves.isEmpty()) {
        handleLeave(leaves, false, null, false);
      }
    }

    
    /**
     * This is used when we are shutting down to inform others
     * Loop: determine coord. If coord is me --> handleLeave().
     * Else send handleLeave() to coord until success
     */
    @Override // GemStoneAddition
    public void leave(Address mbr) {
        Address coord;
        int max_tries=3;
        Object result;

        leave_promise.reset();

        if(mbr.equals(gms.local_addr))
            leaving=true;

        while ((coord=gms.determineCoordinator()) != null && max_tries-- > 0) {
            if(gms.local_addr.equals(coord)) {            // I'm the coordinator
              if (leaving) { // don't know who to tell that we're shutting down - bale out
                break; // GemStoneAddition - bug #42969 hang during shutdown
              }
                gms.becomeCoordinator(this.suspected_mbrs);
                // gms.getImpl().handleLeave(mbr, false);    // regular leave
                gms.getImpl().leave(mbr);    // regular leave
                return;
            }

            if(log.isDebugEnabled()) log.debug("sending LEAVE request to " + coord + " (local_addr=" + gms.local_addr + ")");
            sendLeaveMessage(coord, mbr);

            // GemStoneAddition - if I'm the coordinator
            // don't wait for a response
            
            // GemStoneAddition - scale up the leave_timeout if there are a lot of
            // members
            long leaveTimeout = gms.leave_timeout * gms.members.size() / 10;
            synchronized(leave_promise) {
                result=leave_promise.getResult(leaveTimeout);
                if(result != null)
                    break;
            }

            // GemStoneAddition - we used to only break out of the loop
            // if wouldIBeCoordinator returned true, but we've already
            // sent a ShutdownMessage in GemFire so the other members know
            // that this process shut down normally, so no need to wait
            // for a new view if the coordinator is probably gone

            synchronized(this.suspected_mbrs) {
              if (this.suspected_mbrs.contains(coord)) {
                break;
              }
            }
            
            synchronized(this.departed_mbrs) {
              if (this.departed_mbrs.contains(coord)) {
                break;
              }
            }
            
            // GemStoneAddition - also just quit if the coordinator hasn't
            // changed after LEAVE has already been sent (and probably retransmitted)
            // to it
            if (gms.determineCoordinator() == coord) {
              break;
            }
            
        }
        gms.becomeClient();
    }
    
    public Vector getSuspects() { // GemStoneAddition
      synchronized(this.suspected_mbrs) {
        return new Vector(this.suspected_mbrs);
      }
    }


    @Override // GemStoneAddition
    public void handleJoinResponse(JoinRsp join_rsp) {
        // wrongMethod("handleJoinResponse");
    }

    @Override // GemStoneAddition
    public void handleLeaveResponse(String reason) {
      if (reason != null && reason.length() > 0) {
        gms.passUp(new Event(Event.EXIT,
           gms.stack.gfBasicFunctions.getForcedDisconnectException(
           "This member has been forced out of the distributed system.  Reason='"
               + reason + "'")));
      }
        synchronized(leave_promise) {
            leave_promise.setResult(Boolean.TRUE);  // unblocks thread waiting in leave()
        }
    }


    public void suspect(Address mbr) {
        handleSuspect(mbr);
    }


    /** Removes previously suspected member from list of currently suspected members */
    @Override // GemStoneAddition
    public void unsuspect(Address mbr) {
      synchronized(suspected_mbrs) { // GemStoneAddition
        if(mbr != null)
            suspected_mbrs.remove(mbr);
      }
    }


    @Override // GemStoneAddition
    public void handleJoin(Address mbr) {
    }
    
//    @Override // GemStoneAddition
//    public void handleJoin(List mbrList) { // GemStoneAddition
//    }


    public void handleLeave(Address mbr, boolean suspected) {
      if (suspected) { // GemStoneAddition
        handleSuspect(mbr);
      } else {
        handleLeave(Collections.singletonList(mbr), false, null, false);
      }
    }
    
    // GemStoneAddition - list of mbrs and 'reason'
    @Override // GemStoneAddition
    public void handleLeave(List members, boolean suspected, List reasons, boolean forceInclusion) {
      if (suspected) { // GemStoneAddition
        for (Iterator it=members.iterator(); it.hasNext() && (gms.getImpl() == this); ) {
          handleSuspect((Address)it.next());
        }
      } else {
        boolean becomeCoordinator = false;
        Vector suspects = null;
        Set departed = null;
        synchronized(this.suspected_mbrs) {
          suspects = new Vector(suspected_mbrs);
        }
        synchronized(this.departed_mbrs) {
          this.departed_mbrs.addAll(members);
          departed = new HashSet<Address>(this.departed_mbrs);
        }
        if (wouldIBeCoordinator(suspects, departed)) {
          if(log.isDebugEnabled()) log.debug("suspected mbrs=" + suspected_mbrs + "; departed="+this.departed_mbrs
                + "; members are " +
                gms.members + ", coord=" + gms.local_addr + ": I'm the new coord !");

          becomeCoordinator = true;
        }
        if (becomeCoordinator) {
          synchronized(this.suspected_mbrs) {
            suspected_mbrs.removeAll(suspects);
          }
          synchronized(this.departed_mbrs) {
            this.departed_mbrs.removeAll(departed);
          }
          gms.incrementLtime(10);
          gms.becomeCoordinator(suspects);
        }
      }
    }


    /**
     * If we are leaving, we have to wait for the view change (last msg in the current view) that
     * excludes us before we can leave.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    @Override // GemStoneAddition
    public void handleViewChange(View new_view, Digest digest) {
        Vector mbrs=new_view.getMembers();
         if(log.isDebugEnabled()) log.debug("view=" + new_view);
         synchronized(suspected_mbrs) { // GemStoneAddition
           suspected_mbrs.removeAllElements();
         }
         synchronized(this.departed_mbrs) {
           this.departed_mbrs.clear();
         }
        if(!mbrs.contains(gms.local_addr)) { 
            if (leaving) {
              // received a view in which I'm not member: ignore
              return;
            }
            else {
              // GemStoneChange - if not leaving and we get a LEAVE_RSP, it means
              // this process has been ousted as a suspect
              gms.passUp(new Event(Event.EXIT,
                  gms.stack.gfBasicFunctions.getForcedDisconnectException(
                    ExternalStrings.
                    PGMS_THIS_MEMBER_HAS_BEEN_FORCED_OUT_OF_THE_DISTRIBUTED_SYSTEM_PLEASE_CONSULT_GEMFIRE_LOGS_TO_FIND_THE_REASON_PGMS
                    .toLocalizedString(new_view.getCreator()))));
            }
        }
        gms.installView(new_view, digest);
    }


    public void handleSuspect(Address mbr) {
      boolean becomeCoordinator = false; // GemStoneAddition
      Vector suspects = null;
      if (mbr == null) return;
      
      synchronized (suspected_mbrs) { // GemStoneAddition - bug 35063
          // Both contains() and addElement() are synchronized, but
          // there is a window between the two, so we must synchronize.
          if(!suspected_mbrs.contains(mbr))
            suspected_mbrs.addElement(mbr);
        
        if(log.getLogWriter().fineEnabled()) log.getLogWriter().fine(
            "PGMS: suspected mbr=" + mbr + ", suspected_mbrs=" + suspected_mbrs + ", members=" +
            gms.members + ", local_addr=" + gms.local_addr);

        if(wouldIBeCoordinator(this.suspected_mbrs, Collections.EMPTY_SET)) {
            if(log.isDebugEnabled()) log.debug("suspected mbr=" + mbr + "), departed="+this.departed_mbrs
                + "; members are " +
                    gms.members + ", coord=" + gms.local_addr + ": I'm the new coord !");

            becomeCoordinator = true;
            suspects = new Vector(suspected_mbrs);
            suspected_mbrs.removeAllElements();
        }
      } // synchronized

      if (becomeCoordinator) {
        synchronized(this.departed_mbrs) {
          this.departed_mbrs.clear();
        }
        gms.incrementLtime(10);
        gms.becomeCoordinator(suspects);
        // gms.getImpl().suspect(mbr);
      }
    }

    @Override // GemStoneAddition
    public void handleMergeRequest(Address sender, ViewId merge_id) {
        // only coords handle this method; reject it if we're not coord
        sendMergeRejectedResponse(sender, merge_id);
    }

    /* ---------------------------------- Private Methods --------------------------------------- */

    /**
     * Determines whether this member is the new coordinator given a list of suspected members.  This is
     * computed as follows: the list of currently suspected members (suspected_mbrs) is removed from the current
     * membership. If the first member of the resulting list is equals to the local_addr, then it is true,
     * otherwise false. Example: own address is B, current membership is {A, B, C, D}, suspected members are {A,
     * D}. The resulting list is {B, C}. The first member of {B, C} is B, which is equal to the
     * local_addr. Therefore, true is returned.
     * @param suspects members that have crashed
     * @param departures members that have departed normally
     */
    boolean wouldIBeCoordinator(Vector suspects, Set<Address> departures) {
        Address new_coord;
        Vector mbrs=gms.members.getMembers(); // getMembers() returns a *copy* of the membership vector

        if (log.isDebugEnabled()) {
          log.debug("wouldIBeCoordinator:\nmembers = " + mbrs + "\ndeparted = " + this.departed_mbrs
              + "\nsuspected = " + this.suspected_mbrs);
        }

        if (suspects != null) {
          mbrs.removeAll(suspects);
        }
        
        if (departures != null) {
          mbrs.removeAll(departures);
        }

        if(mbrs.size() < 1) return false;
        // GemStoneAddition - revised for split-brain detection
        new_coord = new Membership(mbrs).getCoordinator();
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "pgms: of members " + mbrs + " the coordinator would be " + new_coord);
        if (new_coord == null) { // oops - no eligable coordinators
          return false;
        }
        return gms.local_addr.equals(new_coord);
    }


    void sendLeaveMessage(Address coord, Address mbr) {
        Message msg=new Message(coord, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, mbr);

        msg.putHeader(gms.getName(), hdr);
        gms.passDown(new Event(Event.MSG, msg));
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


    /* ------------------------------ End of Private Methods ------------------------------------ */

}
