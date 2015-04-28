/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GmsImpl.java,v 1.13 2005/12/23 14:57:06 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;

import com.gemstone.org.jgroups.util.GemFireTracer;

import com.gemstone.org.jgroups.*;

import java.util.List;
import java.util.Vector;





public abstract class GmsImpl {
    protected GMS   gms=null;
    // protected final GemFireTracer  log=GemFireTracer.getLog(getClass());
    protected final GemFireTracer   log;
    final boolean         trace;
    final boolean         warn;
    volatile boolean      leaving=false; // GemStoneAddition - volatile

    protected GmsImpl() {
        log=null;
        trace=warn=false;
    }

    protected GmsImpl(GMS gms) {
        this.gms=gms;
        log=gms.getLog();
        trace=log.isTraceEnabled();
        warn=log.isWarnEnabled();
    }

    public abstract boolean join(Address mbr);
    public abstract void      leave(Address mbr);

    public abstract void      handleJoinResponse(JoinRsp join_rsp);
    public abstract void      handleLeaveResponse(String reason); // GemStoneAddition - added reason

//    public abstract void      suspect(Address mbr);
//    public abstract void suspect(Address mbr, String reason); // GemStoneAddition - added reason
    public abstract void      unsuspect(Address mbr);

    public void               merge(Vector other_coords)                           {} // only processed by coord
    public void               handleMergeRequest(Address sender, ViewId merge_id)  {} // only processed by coords
    public void               handleMergeResponse(MergeData data, ViewId merge_id) {} // only processed by coords
    public void               handleMergeView(MergeData data, ViewId merge_id)     {} // only processed by coords
    public void               handleMergeCancelled(ViewId merge_id)                {} // only processed by coords

    public abstract void handleJoinsAndLeaves(List joins, List leaves, List suspectReqs, List suspectReasons, boolean forceInclusion); // GemStoneAddition
    
    public void                handleGetViewResponse(JoinRsp rsp) {} // GemStoneAddition
    
    public abstract void      handleJoin(Address mbr);
//    public abstract void      handleJoin(List memberList); // GemStoneAddition
    public abstract void handleAlreadyJoined(Address mbr); // GemStoneAddition

    //public abstract void      handleLeave(Address mbr, boolean suspected);
    public abstract void      handleLeave(List memberList, boolean suspected, List reasons, boolean forceInclusion); // GemStoneAddition
    public abstract void      handleViewChange(View new_view, Digest digest);
//    public abstract void      handleSuspect(Address mbr);
    public          void      handleExit() {}

    public boolean            handleUpEvent(Event evt) {return true;}
    public boolean            handleDownEvent(Event evt) {return true;}

    public void               init() throws Exception {leaving=false;}
    public void               start() throws Exception {leaving=false;}
    public void               stop() {leaving=true;}



    protected void sendMergeRejectedResponse(Address sender, ViewId merge_id) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }


    protected void wrongMethod(String method_name) {
        if(log.isWarnEnabled())
            log.warn(method_name + "() should not be invoked on an instance of " + getClass().getName());
    }



    /**
       Returns potential coordinator based on lexicographic ordering of member addresses. Another
       approach would be to keep track of the primary partition and return the first member if we
       are the primary partition.
     */
    protected boolean iWouldBeCoordinator(Vector new_mbrs) {
        Membership tmp_mbrs=gms.members.copy();
        tmp_mbrs.merge(new_mbrs, null);
        tmp_mbrs.sort();
        if(tmp_mbrs.size() <= 0 || gms.local_addr == null)
            return false;
        return gms.local_addr.equals(tmp_mbrs.elementAt(0));
    }

}
