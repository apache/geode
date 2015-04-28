/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: CoordGmsImpl.java,v 1.40 2005/12/23 14:57:06 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;



import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Membership;
import com.gemstone.org.jgroups.MergeView;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.UNICAST;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.TimeScheduler;




/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends GmsImpl  {
    protected/*GemStoneAddition*/ boolean          merging=false;
    private final MergeTask  merge_task=new MergeTask();
    protected/*GemStoneAddition*/ final Vector     merge_rsps=new Vector(11);
    // for MERGE_REQ/MERGE_RSP correlation, contains MergeData elements
    protected/*GemStoneAddition*/ ViewId           merge_id=null;

    protected/*GemStoneAddition*/ Address          merge_leader=null;

    private MergeCanceller   merge_canceller=null;

    private final Object     merge_canceller_mutex=new Object();
    
    private final Object view_mutex = new Object(); // GemStoneAddition - protect view processing


    public CoordGmsImpl(GMS g) {
        super(g);
    }


    protected/*GemStoneAddition*/ void setMergeId(ViewId merge_id) {
        this.merge_id=merge_id;
        synchronized(merge_canceller_mutex) {
            if(this.merge_id != null) {
                stopMergeCanceller();
                merge_canceller=new MergeCanceller(this.merge_id, gms.merge_timeout);
                gms.timer.add(merge_canceller);
            }
            else { // merge completed
                stopMergeCanceller();
            }
        }
    }

    protected/*GemStoneAddition*/ void stopMergeCanceller() {
        synchronized(merge_canceller_mutex) {
            if(merge_canceller != null) {
                merge_canceller.cancel();
                merge_canceller=null;
            }
        }
    }

    @Override // GemStoneAddition
    public void init() throws Exception {
        super.init();
        cancelMerge();
    }

    @Override // GemStoneAddition
    public boolean join(Address mbr) { // GemStoneAddition - return boolean
        wrongMethod("join");
        return false;
    }

    /** The coordinator itself wants to leave the group */
    @Override // GemStoneAddition
    public void leave(Address mbr) {
      synchronized (view_mutex) { // GemStoneAddition
        if(mbr == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_MEMBERS_ADDRESS_IS_NULL_);
            return;
        }
        // GemStoneAddition -- handle leave of the coordinator directly,
        // don't try to push it through the view_handler
        if(mbr.equals(gms.local_addr)) {
            leaving=true;
            handleLeave(mbr, false, ""); // regular leave
        }
        else {
          gms.view_handler.add(new GMS.Request(GMS.Request.LEAVE, mbr, false, null));
          gms.view_handler.stop(true); // wait until all requests have been processed, then close the queue and leave
          gms.view_handler.waitUntilCompleted(gms.leave_timeout);
        }
      } // synchronized
    }

    @Override // GemStoneAddition
    public void handleJoinResponse(JoinRsp join_rsp) {
        wrongMethod("handleJoinResponse");
    }

    @Override // GemStoneAddition
    public void handleLeaveResponse(String reason) {
    }

//    public void suspect(Address mbr) {
//        handleJoinsAndLeaves(Collections.EMPTY_LIST, Collections.EMPTY_LIST,
//            Collections.singletonList(mbr), false);
//    }
    
    @Override // GemStoneAddition
    public void unsuspect(Address mbr) {

    }

    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param other_coords A list of coordinators (including myself) found by MERGE protocol
     */
    @Override // GemStoneAddition
    public void merge(Vector other_coords) {
      synchronized(view_mutex) { // GemStoneAddition
        Membership tmp;

        if(merging) {
            if(warn) log.warn("merge already in progress, discarded MERGE event (I am " + gms.local_addr + ")");
            return;
        }
        merge_leader=null;
        if(other_coords == null) {
            if(warn) log.warn("list of other coordinators is null. Will not start merge.");
            return;
        }

        if(other_coords.size() <= 1) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.CoordGmsImpl_NUMBER_OF_COORDINATORS_FOUND_IS__0__WILL_NOT_PERFORM_MERGE, other_coords.size());
            return;
        }

        /* Establish deterministic order, so that coords can elect leader */
        tmp=new Membership(other_coords);
        tmp.sort();
        merge_leader=(Address)tmp.elementAt(0);
        // if(log.isDebugEnabled()) log.debug("coordinators in merge protocol are: " + tmp);
        if(merge_leader.equals(gms.local_addr) || gms.merge_leader) {
            if(trace)
                log.trace("I (" + gms.local_addr + ") will be the leader. Starting the merge task");
            startMergeTask(other_coords);
        }
        else {
            if(trace) log.trace("I (" + gms.local_addr + ") am not the merge leader, " +
                    "waiting for merge leader (" + merge_leader + ")to initiate merge");
        }
      } // synchronized(view_mutex)
    }

    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     */
    @Override // GemStoneAddition
    public void handleMergeRequest(Address sender, ViewId merge_id) {
      synchronized(view_mutex) { // GemStoneAddition
        Digest digest;
        View view;

        if(sender == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_SENDER__NULL_CANNOT_SEND_BACK_A_RESPONSE);
            return;
        }
        if(merging) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_MERGE_ALREADY_IN_PROGRESS);
            sendMergeRejectedResponse(sender, merge_id);
            return;
        }
        merging=true;

        /* Clears the view handler queue and discards all JOIN/LEAVE/MERGE requests until after the MERGE  */
        gms.view_handler.suspend(merge_id);

        setMergeId(merge_id);
        if(log.isDebugEnabled()) log.debug("sender=" + sender + ", merge_id=" + merge_id);
        digest=gms.getDigest();
        synchronized(gms.members) { // GemStoneAddition - synch
          view=new View(gms.view_id.copy(), gms.members.getMembers());
        }
        gms.passDown(new Event(Event.ENABLE_UNICASTS_TO, sender));
        sendMergeResponse(sender, view, digest);
      }
    }


    private MergeData getMergeResponse(Address sender, ViewId merge_id) {
        Digest         digest;
        View           view;
        MergeData      retval;

        if(sender == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_SENDER__NULL_CANNOT_SEND_BACK_A_RESPONSE);
            return null;
        }
        if(merging) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_MERGE_ALREADY_IN_PROGRESS);
            retval=new MergeData(sender, null, null);
            retval.merge_rejected=true;
            return retval;
        }
        merging=true;
        setMergeId(merge_id);
        if(log.isDebugEnabled()) log.debug("sender=" + sender + ", merge_id=" + merge_id);

        try {
            digest=gms.getDigest();
            synchronized(gms.members) { // GemStoneAddition - synch
              view=new View(gms.view_id.copy(), gms.members.getMembers());
            }
            retval=new MergeData(sender, view, digest);
            retval.view=view;
            retval.digest=digest;
        }
        catch(NullPointerException null_ex) {
            return null;
        }
        return retval;
    }


    @Override // GemStoneAddition
    public void handleMergeResponse(MergeData data, ViewId merge_id) {
      synchronized(view_mutex) { // GemStoneAddition
        if(data == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_MERGE_DATA_IS_NULL);
            return;
        }
        if(merge_id == null || this.merge_id == null) {
            if(log.isErrorEnabled())
                log.error("merge_id ("
                    + merge_id
                    + ") or this.merge_id ("
                    + this.merge_id
                    + ") is null (sender="
                    + data.getSender()
                    + ").");
            return;
        }

        if(!this.merge_id.equals(merge_id)) {
            if(log.isErrorEnabled()) log.error("this.merge_id ("
                    + this.merge_id
                    + ") is different from merge_id ("
                    + merge_id
                    + ')');
            return;
        }

        synchronized(merge_rsps) {
            if(!merge_rsps.contains(data)) {
                merge_rsps.addElement(data);
                merge_rsps.notifyAll();
            }
        }
      }
    }

    /**
     * If merge_id is not equal to this.merge_id then discard.
     * Else cast the view/digest to all members of this group.
     */
    @Override // GemStoneAddition
    public void handleMergeView(MergeData data, ViewId merge_id) {
      synchronized(view_mutex) {// GemStoneAddition
        if(merge_id == null
                || this.merge_id == null
                || !this.merge_id.equals(merge_id)) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_MERGE_IDS_DONT_MATCH_OR_ARE_NULL_MERGE_VIEW_DISCARDED);
            return;
        }
        java.util.List my_members=gms.view != null? gms.view.getMembers() : null;

        // only send to our *current* members, if we have A and B being merged (we are B), then we would *not*
        // receive a VIEW_ACK from A because A doesn't see us in the pre-merge view yet and discards the view

        GMS.Request req=new GMS.Request(GMS.Request.VIEW);
        req.view=data.view;
        req.digest=data.digest;
        req.target_members=my_members;
        gms.view_handler.add(req, true, // at head so it is processed next
                             true);     // un-suspend the queue
        merging=false;
      }
    }

    @Override // GemStoneAddition
    public void handleMergeCancelled(ViewId merge_id) {
      synchronized(view_mutex) { // GemStoneAddition
        if(merge_id != null
                && this.merge_id != null
                && this.merge_id.equals(merge_id)) {
            if(log.isDebugEnabled()) log.debug("merge was cancelled (merge_id=" + merge_id + ", local_addr=" +
                    gms.local_addr +")");
            setMergeId(null);
            this.merge_leader=null;
            merging=false;
            gms.view_handler.resume(merge_id);
        }
      }
    }


    protected/*GemStoneAddition*/ void cancelMerge() {
      synchronized(view_mutex) { // GemStoneAddition
        Object tmp=merge_id;
        if(merge_id != null && log.isDebugEnabled()) log.debug("cancelling merge (merge_id=" + merge_id + ')');
        setMergeId(null);
        this.merge_leader=null;
        stopMergeTask();
        merging=false;
        synchronized(merge_rsps) {
            merge_rsps.clear();
        }
        gms.view_handler.resume(tmp);
      }
    }
    
    
    // GemStoneAddition - added in 7.0 to allow joins and leaves to be processed
    // in the same view change
    @Override
    public void handleJoinsAndLeaves(List joins, List leaves, List suspects, List suspectReasons, boolean forceInclusion) {
      // handleJoin creates a view and sends it in join responses, so we do
      // leave/suspect processing first
      Vector left = _handleLeave(leaves, false, Collections.EMPTY_LIST, forceInclusion);
      Vector s = _handleLeave(suspects, true, suspectReasons, forceInclusion);
      View joinView = handleJoin(joins, left, s);
      if (joinView == null) {
        gms.castViewChange(null, left, s, true);
      } else {
        gms.castViewChange(joinView, null, true);
      }
    }

    /** GemStoneAddition - we now always process a list */
    @Override // GemStoneAddition
    public synchronized void handleJoin(Address mbr) {
      throw new UnsupportedOperationException("This version of the method is no longer used");
    }


    /**
     * Computes the new view (including the newly joined member) and get the digest from PBCAST.
     * Returns both in the form of a JoinRsp
     */
  private synchronized View handleJoin(List members, Vector left, Vector suspect) {
    synchronized(view_mutex) { // GemStoneAddition
      if (leaving) { // GemStoneAddition - don't send new view if shutting down
        return null;
      }
      View v;
      Digest d, tmp;
      JoinRsp join_rsp;

//log.getLogWriter().info("processing new member " + mbr); // debugging
//        if(mbr == null) {
//            if(log.isErrorEnabled()) log.error("mbr is null");
//            return;
//        }
//        if(gms.local_addr.equals(mbr)) {
//            if(log.isErrorEnabled()) log.error("cannot join myself !");
//            return;
//        }
      Vector new_mbrs=new Vector(1);

      tmp=gms.getDigest(); // get existing digest
      if(tmp == null) {
          if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_RECEIVED_NULL_DIGEST_FROM_GET_DIGEST_WILL_CAUSE_JOIN_TO_FAIL);
          return null;
      }

      d=new Digest(tmp.size() + new_mbrs.size()); // create a new digest, which contains 1 more member
      d.add(tmp); // add the existing digest to the new one

      // GemStoneAddition - gms.members must only be accessed under synchronization
      // since it is cleared in the set() method
      Membership mbrs = new Membership();
//      View view;
//      ViewId vid;
//      Digest digest;
      synchronized(gms.members) {
        mbrs.set(gms.members);
//        view = gms.view;
//        vid = gms.view_id;
//        digest = gms.getDigest();
      }
      boolean needNewView = false;
      for (Iterator it = members.iterator(); it.hasNext(); ) {
        IpAddress mbr = (IpAddress)it.next();
        if(log.isDebugEnabled()) log.debug("mbr joining=" + mbr);
        // GemStoneAddition - don't allow it in if there's a member with the same ID already present.
        // This will trigger a retry in the sender while the old ID is being removed by the
        // view processor
        if (mbr.getBirthViewId() < 0 && mbrs.contains(mbr)) {
          log.getLogWriter().info(
            ExternalStrings. COORDGMSIMPL_REJECTING_0_DUE_TO_REUSED_IDENTITY, mbr);
          sendJoinResponse(new JoinRsp(JoinRsp.SHUNNED_ADDRESS), mbr, false);
          continue;
        }
        // GemstoneAddition - don't allow addr to be reused if it's still in the view
        boolean found = false;
        boolean shunned = false;
        for (Iterator mit=mbrs.getMembers().iterator(); !found && !shunned && mit.hasNext(); ) {
          Address m = (Address)mit.next();
          if (m.equals(mbr)) {
            if ( ((IpAddress)m).getUniqueID() != mbr.getUniqueID()) {
              sendJoinResponse(new JoinRsp(JoinRsp.SHUNNED_ADDRESS), mbr, false);
              shunned = true;
            } else {
              found = true;
            }
          }
        }
        if (!shunned) {
          if (found) { // already joined: return current digest and membership
            handleAlreadyJoined(mbr);
          }
          else {
            needNewView = true;
            new_mbrs.addElement(mbr);
            d.add(mbr, 0, 0); // ... and add the new member. it's first seqno will be 1
          }
        }
      }

      if (needNewView) {
        v=gms.getNextView(new_mbrs, left, suspect);
        for (Iterator it = new_mbrs.iterator(); it.hasNext(); ) {
          IpAddress mbr = (IpAddress)it.next();
          mbr.setBirthViewId(v.getVid().getId()); // GemStoneAddition - vid in ID
        }
        if(log.isDebugEnabled()) log.debug("joined members " + new_mbrs + ", view is " + v);
//log.getLogWriter().info("joined member " + mbr + ", view is " + v); // debugging
        join_rsp=new JoinRsp(v, d);

        // 2. Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        //    in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details
        if(join_rsp.getView() != null)
            gms.passDown(new Event(Event.TMP_VIEW, join_rsp.getView()));

          // 3. Return result to client
        for (Iterator it = new_mbrs.iterator(); it.hasNext(); ) { // GemStoneAddition - use new_mbrs instead of members here
          sendJoinResponse(join_rsp, (Address)it.next(), true);
        }
      
//        // 4. Broadcast the new view
//        if (join_rsp.getView() != null) // GemStoneAddition
//          gms.castViewChange(join_rsp.getView(), null, true);
        return join_rsp.getView();
        }
      }
      return null;
    }


  /**
   * GemStoneAddition - process a Join request from a process that's already
   * a member.  We head this off in GMS and don't pass it through the
   * ViewHandler queue
   * @param mbr
   */
    @Override // GemStoneAddition
  public void handleAlreadyJoined(Address mbr) {
    Membership mbrs = new Membership();
    View view;
    ViewId vid;
    Digest digest;
    synchronized(gms.members) {
      mbrs.set(gms.members);
      view = gms.view;
      vid = gms.view_id;
      digest = gms.getDigest();
    }
    if(log.getLogWriter().warningEnabled()) {
      log.getLogWriter().warning(
          ExternalStrings.COORDGMSIMPL_0_ALREADY_PRESENT_RETURNING_EXISTING_VIEW_1,
          new Object[] {mbr, view });
    }
    JoinRsp join_rsp=new JoinRsp(new View(vid, mbrs.getMembers()), digest);
    sendJoinResponse(join_rsp, mbr, true);
  }



    /**
      Exclude <code>mbr</code> from the membership. If <code>suspected</code> is true, then
      this member crashed and therefore is forced to leave, otherwise it is leaving voluntarily.
      */
     public void handleLeave(Address mbr, boolean suspected, String reason) { // GemStoneAddition - added 'reason'
       List mbrs = Collections.singletonList(mbr);
       List reasons = Collections.singletonList(reason);
       handleLeave(mbrs, suspected, reasons, false);
     }
     
     @Override
     public void handleLeave(List mbrs, boolean suspected, List reasons, boolean forceInclusion) {
       if (suspected) {
         handleJoinsAndLeaves(Collections.EMPTY_LIST, Collections.EMPTY_LIST, mbrs, reasons, forceInclusion);
       } else {
         handleJoinsAndLeaves(Collections.EMPTY_LIST, mbrs, Collections.EMPTY_LIST, Collections.EMPTY_LIST, forceInclusion);
       }
     }
     
     private Vector _handleLeave(List mbrs, boolean suspected, List reasons, boolean forceInclusion) { // GemStoneAddition - handle list of mbrs and added 'reason'
       synchronized(this.gms.stack.getChannel()) { // GemStoneAddition - bug #42969 lock inversion with JChannel.close()
       synchronized(view_mutex) { // GemStoneAddition
         if (leaving) { // GemStoneAddition
           if ( ! (mbrs.size() == 1 && mbrs.contains(gms.local_addr)) ) {
             return null;
           }
         }
         Vector v=new Vector(mbrs.size());
         // contains either leaving mbrs or suspected mbrs
//         if(log.isDebugEnabled()) log.debug("mbr=" + mbr);
//         if(!gms.members.contains(mbr)) {
//             if(trace) log.trace("mbr " + mbr + " is not a member !");
//             return;
//         }

         if(gms.view_id == null) {
             // we're probably not the coord anymore (we just left ourselves), let someone else do it
             // (client will retry when it doesn't get a response
             if(log.isDebugEnabled())
                 log.debug("gms.view_id is null, I'm not the coordinator anymore (leaving=" + leaving +
                           "); the new coordinator will handle the leave request");
             return null;
         }

         
         // GemStoneAddition - suspect set must have full info in addresses, so
         // pull them from the membership set instead of just using whatever
         // was passed to this method
         Map realAddresses = new HashMap();
         
         Vector members = gms.members.getMembers();
         synchronized(members) {
           for (Iterator it=members.iterator(); it.hasNext(); ) {
             Object addr = it.next();
             realAddresses.put(addr, addr);
           }
         }

         Iterator reasonIterator = reasons.iterator();
         String reason = "";
         for (Iterator it = mbrs.iterator(); it.hasNext(); ) {
           Address mbr = (Address)it.next();

           Address realMbr = (Address)realAddresses.get(mbr);
           if (realMbr != null) {
             mbr = realMbr;
           }
           if (suspected && reasonIterator.hasNext()) { // bug #44928 - NoSuchElementException
             reason = (String)reasonIterator.next();
           }
           if (suspected && mbr.equals(gms.local_addr)) {
             // GemStoneAddition - if this member is suspected, it is required for
             // force-disconnect functionality that it terminate its channel now
             if (reason.length() > 0) {
               log.getLogWriter().info(ExternalStrings.COORDGMSIMPL_I_AM_BEING_REMOVED_FROM_MEMBERSHIP_0, reason);
             }
             gms.passUp(new Event(Event.EXIT, gms.stack.gfBasicFunctions.getForcedDisconnectException(
                 ExternalStrings.COORDGMSIMPL_THIS_MEMBER_HAS_BEEN_FORCED_OUT_OF_THE_DISTRIBUTED_SYSTEM_0_CGMS
                   .toLocalizedString( new Object[] {
                     (reason != null && reason.length() > 0? "Reason='" + reason +
                     "'" : ExternalStrings.COORDGMSIMPL_PLEASE_CONSULT_GEMFIRE_LOGS_TO_FIND_THE_REASON.toLocalizedString())
                 }))));
               //if(warn) log.warn("I am the coord and I'm being am suspected -- will probably leave shortly");
               return null;
           }

           sendLeaveResponse(mbr, reason); // send an ack to the leaving member
           
           if (realMbr != null) { // GemStoneAddition - don't include if it wasn't a member
             v.addElement(realMbr);
           } else if (forceInclusion) {
             v.add(mbr);
           }
         }
         
         return v;
       }
       }
     }




    void sendLeaveResponse(Address mbr, String reason) {
        Message msg=new Message(mbr, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_RSP, true, reason, mbr);
        msg.putHeader(gms.getName(), hdr);
        // since it's no longer in membership, don't use retransmission logic
        msg.putHeader(UNICAST.BYPASS_UNICAST, hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    @Override // GemStoneAddition
    public void handleViewChange(View new_view, Digest digest) {
        Vector mbrs=new_view.getMembers();
        if(log.isDebugEnabled()) {
            if(digest != null)
                log.debug("view=" + new_view + ", digest=" + digest);
            else
                log.debug("view=" + new_view);
        }

        if(leaving && !mbrs.contains(gms.local_addr))
            return;
        gms.installView(new_view, digest);
    }
    
//    @Override // GemStoneAddition
//    public void handleSuspect(Address mbr) {
//      suspect(mbr, "");
//    }

//    public void handleSuspect(Address mbr, String reason) {
//        handleLeave(mbr, true, reason); // irregular leave - forced
//    }

    @Override // GemStoneAddition
    public void handleExit() {
        cancelMerge();
    }

    @Override // GemStoneAddition
    public void stop() {
        super.stop(); // sets leaving=false
        stopMergeTask();
    }

    /* ------------------------------------------ Private methods ----------------------------------------- */

    void startMergeTask(Vector coords) {
        synchronized(merge_task) {
            merge_task.start(coords);
        }
    }

    void stopMergeTask() {
        synchronized(merge_task) {
            merge_task.stop();
        }
    }

    void sendJoinResponse(JoinRsp rsp, Address dest, boolean isJoined) {
        Message m=new Message(dest, null, null);
        m.isHighPriority = true; // GemStoneAddition - bypass queues
        m.isJoinResponse = true; // GemStoneAddition - debugging 39744
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP);
        m.putHeader(gms.getName(), hdr);
        m.setObject(rsp);
        // GemStoneAddition - if not a member, don't use UNICAST because it may not
        // send the message
        if (!isJoined) {
          // doesn't matter what the value of this header is, so keep it small
          m.putHeader(UNICAST.BYPASS_UNICAST, new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP));
          gms.passDown(new Event(Event.DISABLE_UNICASTS_TO, dest));
        }
        gms.passDown(new Event(Event.MSG, m));
    }

    /**
     * Sends a MERGE_REQ to all coords and populates a list of MergeData (in merge_rsps). Returns after coords.size()
     * response have been received, or timeout msecs have elapsed (whichever is first).<p>
     * If a subgroup coordinator rejects the MERGE_REQ (e.g. because of participation in a different merge),
     * <em>that member will be removed from coords !</em>
     * @param coords A list of Addresses of subgroup coordinators (inluding myself)
     * @param timeout Max number of msecs to wait for the merge responses from the subgroup coords
     */
    protected/*GemStoneAddition*/ void getMergeDataFromSubgroupCoordinators(Vector coords, long timeout) {
        Message msg;
        GMS.GmsHeader hdr;

        long curr_time, time_to_wait, end_time, start, stop;
        int num_rsps_expected;

        if(coords == null || coords.size() <= 1) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_COORDS__NULL_OR_SIZE__1);
            return;
        }

        start=System.currentTimeMillis();
        MergeData tmp;
        synchronized(merge_rsps) {
            merge_rsps.removeAllElements();
            if(log.isDebugEnabled()) log.debug("sending MERGE_REQ to " + coords);
            Address coord;
            for(int i=0; i < coords.size(); i++) {
                coord=(Address)coords.elementAt(i);
                if(gms.local_addr != null && gms.local_addr.equals(coord)) {
                    tmp=getMergeResponse(gms.local_addr, merge_id);
                    if(tmp != null)
                        merge_rsps.add(tmp);
                    continue;
                }

                // this allows UNICAST to remove coord from previous_members in case of a merge
                gms.passDown(new Event(Event.ENABLE_UNICASTS_TO, coord));

                msg=new Message(coord, null, null);
                hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
                hdr.mbr=gms.local_addr;
                hdr.merge_id=merge_id;
                msg.putHeader(gms.getName(), hdr);
                gms.passDown(new Event(Event.MSG, msg));
            }

            // wait until num_rsps_expected >= num_rsps or timeout elapsed
            num_rsps_expected=coords.size();
            curr_time=System.currentTimeMillis();
            end_time=curr_time + timeout;
            while(end_time > curr_time) {
                time_to_wait=end_time - curr_time;
                if(log.isDebugEnabled()) log.debug("waiting " + time_to_wait + " msecs for merge responses");
                if(merge_rsps.size() < num_rsps_expected) {
                    boolean interrupted = Thread.interrupted(); // GemStoneAddition
                    try {
                        merge_rsps.wait(time_to_wait);
                    }
                    catch(InterruptedException ex) { // GemStoneAddition
                      interrupted = true;
                    }
                    finally { // GemStoneAddition
                      if (interrupted) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    if (interrupted) {
                      break; // treat interrupt like timeout
                    }
                }
                if(log.isDebugEnabled())
                    log.debug("num_rsps_expected=" + num_rsps_expected + ", actual responses=" + merge_rsps.size());

                if(merge_rsps.size() >= num_rsps_expected)
                    break;
                curr_time=System.currentTimeMillis();
            }
            stop=System.currentTimeMillis();
            if(trace)
                log.trace("collected " + merge_rsps.size() + " merge response(s) in " + (stop-start) + "ms");
        }
    }

    /**
     * Generates a unique merge id by taking the local address and the current time
     */
    protected/*GemStoneAddition*/ ViewId generateMergeId() {
        return new ViewId(gms.local_addr, System.currentTimeMillis());
        // we're (ab)using ViewId as a merge id
    }

    /**
     * Merge all MergeData. All MergeData elements should be disjunct (both views and digests). However,
     * this method is prepared to resolve duplicate entries (for the same member). Resolution strategy for
     * views is to merge only 1 of the duplicate members. Resolution strategy for digests is to take the higher
     * seqnos for duplicate digests.<p>
     * After merging all members into a Membership and subsequent sorting, the first member of the sorted membership
     * will be the new coordinator.
     * @param v A list of MergeData items. Elements with merge_rejected=true were removed before. Is guaranteed
     *          not to be null and to contain at least 1 member.
     */
    protected/*GemStoneAddition*/ MergeData consolidateMergeData(Vector v) {
        MergeData ret;
        MergeData tmp_data;
        long logical_time=0; // for new_vid
        ViewId new_vid, tmp_vid;
        MergeView new_view;
        View tmp_view;
        Membership new_mbrs=new Membership();
        int num_mbrs;
        Digest new_digest;
        Address new_coord;
        Vector subgroups=new Vector(11);
        // contains a list of Views, each View is a subgroup

        for(int i=0; i < v.size(); i++) {
            tmp_data=(MergeData)v.elementAt(i);
            if(log.isDebugEnabled()) log.debug("merge data is " + tmp_data);
            tmp_view=tmp_data.getView();
            if(tmp_view != null) {
                tmp_vid=tmp_view.getVid();
                if(tmp_vid != null) {
                    // compute the new view id (max of all vids +1)
                    logical_time=Math.max(logical_time, tmp_vid.getId());
                }
                // merge all membership lists into one (prevent duplicates)
                new_mbrs.add(tmp_view.getMembers());
                subgroups.addElement(tmp_view.clone());
            }
        }

        // the new coordinator is the first member of the consolidated & sorted membership list
        new_mbrs.sort();
        num_mbrs=new_mbrs.size();
        new_coord=num_mbrs > 0? (Address)new_mbrs.elementAt(0) : null;
        if(new_coord == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_NEW_COORD__NULL);
            return null;
        }
        // should be the highest view ID seen up to now plus 1
        new_vid=new ViewId(new_coord, logical_time + 1);

        // determine the new view
        new_view=new MergeView(new_vid, new_mbrs.getMembers(), subgroups);
        if(log.isDebugEnabled()) log.debug("new merged view will be " + new_view);

        // determine the new digest
        new_digest=consolidateDigests(v, num_mbrs);
        if(new_digest == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_DIGEST_COULD_NOT_BE_CONSOLIDATED);
            return null;
        }
        if(log.isDebugEnabled()) log.debug("consolidated digest=" + new_digest);
        ret=new MergeData(gms.local_addr, new_view, new_digest);
        return ret;
    }

    /**
     * Merge all digests into one. For each sender, the new value is min(low_seqno), max(high_seqno),
     * max(high_seqno_seen)
     */
    private Digest consolidateDigests(Vector v, int num_mbrs) {
        MergeData data;
        Digest tmp_digest, retval=new Digest(num_mbrs);

        for(int i=0; i < v.size(); i++) {
            data=(MergeData)v.elementAt(i);
            tmp_digest=data.getDigest();
            if(tmp_digest == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_TMP_DIGEST__NULL_SKIPPING);
                continue;
            }
            retval.merge(tmp_digest);
        }
        return retval;
    }

    /**
     * Sends the new view and digest to all subgroup coordinors in coords. Each coord will in turn
     * <ol>
     * <li>cast the new view and digest to all the members of its subgroup (MergeView)
     * <li>on reception of the view, if it is a MergeView, each member will set the digest and install
     *     the new view
     * </ol>
     */
    protected/*GemStoneAddition*/ void sendMergeView(Vector coords, MergeData combined_merge_data) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;
        View v;
        Digest d;

        if(coords == null || combined_merge_data == null)
            return;

        v=combined_merge_data.view;
        d=combined_merge_data.digest;
        if(v == null || d == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_VIEW_OR_DIGEST_IS_NULL_CANNOT_SEND_CONSOLIDATED_MERGE_VIEWDIGEST);
            return;
        }

        if(trace)
            log.trace("sending merge view " + v.getVid() + " to coordinators " + coords);

        for(int i=0; i < coords.size(); i++) {
            coord=(Address)coords.elementAt(i);
            msg=new Message(coord, null, null);
            hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW);
//            hdr.my_digest=d;
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            v.setMessageDigest(d);
            msg.setObject(v);
            gms.passDown(new Event(Event.MSG, msg));
        }
    }

    /**
     * Send back a response containing view and digest to sender
     */
    private void sendMergeResponse(Address sender, View view, Digest digest) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_id=merge_id;
//        hdr.my_digest=digest;
        msg.putHeader(gms.getName(), hdr);
        view.setMessageDigest(digest);
        msg.setObject(view);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }


    protected/*GemStoneAddition*/ void sendMergeCancelledMessage(Vector coords, ViewId merge_id) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;

        if(coords == null || merge_id == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_COORDS_OR_MERGE_ID__NULL);
            return;
        }
        for(int i=0; i < coords.size(); i++) {
            coord=(Address)coords.elementAt(i);
            msg=new Message(coord, null, null);
            hdr=new GMS.GmsHeader(GMS.GmsHeader.CANCEL_MERGE);
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.passDown(new Event(Event.MSG, msg));
        }
    }

    /** Removed rejected merge requests from merge_rsps and coords */
    protected/*GemStoneAddition*/ void removeRejectedMergeRequests(Vector coords) {
        MergeData data;
        for(Iterator it=merge_rsps.iterator(); it.hasNext();) {
            data=(MergeData)it.next();
            if(data.merge_rejected) {
                if(data.getSender() != null && coords != null)
                    coords.removeElement(data.getSender());
                it.remove();
                if(log.isDebugEnabled()) log.debug("removed element " + data);
            }
        }
    }

    /* --------------------------------------- End of Private methods ------------------------------------- */

    /**
     * Starts the merge protocol (only run by the merge leader). Essentially sends a MERGE_REQ to all
     * coordinators of all subgroups found. Each coord receives its digest and view and returns it.
     * The leader then computes the digest and view for the new group from the return values. Finally, it
     * sends this merged view/digest to all subgroup coordinators; each coordinator will install it in their
     * subgroup.
     */
    protected/*GemStoneAddition*/ class MergeTask implements Runnable {
        Thread t=null; // GemStoneAddition -- synchronized on this
        Vector coords=null; // list of subgroup coordinators to be contacted

        synchronized /* GemStoneAddition */ public void start(Vector coords) {
            if(t == null || !t.isAlive()) {
                this.coords=(Vector)(coords != null? coords.clone() : null);
                t=new Thread(GemFireTracer.GROUP, this, "MergeTask");
                t.setDaemon(true);
                t.start();
            }
        }

        synchronized /* GemStoneAddition */ public void stop() {
            Thread tmp=t;
            if(isRunning()) {
                t=null;
                tmp.interrupt();
            }
            t=null;
            coords=null;
        }

        public boolean isRunning() {
          synchronized (this) { // GemStoneAddition
            return t != null && t.isAlive();
          }
        }

        /**
         * Runs the merge protocol as a leader
         */
        public void run() {
            MergeData combined_merge_data;

            if(merging == true) {
                if(warn) log.warn("merge is already in progress, terminating");
                return;
            }

            if(log.isDebugEnabled()) log.debug("merge task started, coordinators are " + this.coords);
            try {

                /* 1. Generate a merge_id that uniquely identifies the merge in progress */
                setMergeId(generateMergeId());

                /* 2. Fetch the current Views/Digests from all subgroup coordinators */
                getMergeDataFromSubgroupCoordinators(coords, gms.merge_timeout);

                /* 3. Remove rejected MergeData elements from merge_rsp and coords (so we'll send the new view only
                   to members who accepted the merge request) */
                removeRejectedMergeRequests(coords);

                if(merge_rsps.size() <= 1) {
                    if(warn)
                        log.warn("merge responses from subgroup coordinators <= 1 (" + merge_rsps + "). Cancelling merge");
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 4. Combine all views and digests into 1 View/1 Digest */
                combined_merge_data=consolidateMergeData(merge_rsps);
                if(combined_merge_data == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_COMBINED_MERGE_DATA__NULL);
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 5. Don't allow JOINs or LEAVEs until we are done with the merge. Suspend() will clear the
                      view handler queue, so no requests beyond this current MERGE request will be processed */
                gms.view_handler.suspend(merge_id);

                /* 6. Send the new View/Digest to all coordinators (including myself). On reception, they will
                   install the digest and view in all of their subgroup members */
                sendMergeView(coords, combined_merge_data);
            }
            catch(RuntimeException ex) {
              if(log.isErrorEnabled()) log.error(ExternalStrings.CoordGmsImpl_EXCEPTION_WHILE_MERGING, ex);
            } 
            finally {
                sendMergeCancelledMessage(coords, merge_id);
                stopMergeCanceller(); // this is probably not necessary
                merging=false;
                merge_leader=null;
                if(log.isDebugEnabled()) log.debug("merge task terminated");
//                t=null; GemStoneAddition -- let stop() do this.
            }
        }
    }


    private class MergeCanceller implements TimeScheduler.Task {
        private Object my_merge_id=null;
        private long timeout;
        private boolean cancelled=false;

        MergeCanceller(Object my_merge_id, long timeout) {
            this.my_merge_id=my_merge_id;
            this.timeout=timeout;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public void cancel() {
            cancelled=true;
        }

        public long nextInterval() {
            return timeout;
        }

        public void run() {
            if(merge_id != null && my_merge_id.equals(merge_id)) {
                if(trace)
                    log.trace("cancelling merge due to timer timeout (" + timeout + " ms)");
                cancelMerge();
                cancelled=true;
            }
            else {
                if(trace)
                    log.trace("timer kicked in after " + timeout + " ms, but no (or different) merge was in progress: " +
                              "merge_id=" + merge_id + ", my_merge_id=" + my_merge_id);
            }
        }
    }

}
