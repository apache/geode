/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.TimeoutException;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.6 2005/11/18 15:12:24 belaban Exp $
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    private java.util.List missing_acks;
    private Set            received_acks=new HashSet();
    private final Promise        all_acks_received=new Promise();
    private ViewId               proposed_view;
    private Set            suspected_mbrs=new HashSet();


    public AckCollector() {
        missing_acks=new ArrayList();
    }

    public AckCollector(ViewId v, java.util.List l) {
        missing_acks=new ArrayList(l);
        proposed_view=v;
    }

    public java.util.List getMissing() {
        return missing_acks;
    }

    public Set getReceived() {
        return received_acks;
    }

    public ViewId getViewId() {
        return proposed_view;
    }

    public void reset(ViewId v, java.util.Collection l) {
        proposed_view=v;
        missing_acks = new ArrayList(); // GemStoneAddition - clearing array list caused corruption in IBM 1.6.0 jvm
//        received_acks.clear(); GemStoneAddition
        if(l != null)
            missing_acks.addAll(l);
        missing_acks.removeAll(suspected_mbrs);
        missing_acks.removeAll(received_acks); // GemStoneAddition
        received_acks.addAll(suspected_mbrs); // GemStoneAddition
        all_acks_received.reset();
        if (missing_acks.size() == 0) { // GemStoneAddition - early out if no non-suspects
          all_acks_received.setResult(Boolean.TRUE);
        }
    }
    
    /**
     * GemStoneAddition - prior to adding fullyReset and reworking reset(),
     * JGroups would receive VIEW_ACKS from members processing a JOIN_RSP
     * and, if castViewChangeWithDest had not started processing, it would
     * throw these acks away when the collector was reset.  Now we leave it
     * in a fullyReset state after we finish processing a view and don't
     * clear the received_acks collection until the next view is fully
     * processed.<br>
     * Current JGroups source (as of 11/21/08) handle this problem by deferring
     * the transmission of JOIN_RSP messages until after the AckCollector
     * has been reset.
     */
    public void fullyReset() {
      received_acks = new HashSet();
      suspected_mbrs = new HashSet(); // GemStoneAddition
    }

    public int size() {
        return missing_acks.size();
    }

    public void ack(Object member, ViewId vid) { // GemStoneAddition - added vid parameter
      // GemStoneAddition - bug #39429, do not count the ack if it's not
      // intended for this view.  If there are ack timeouts followed by a new
      // view then overlap from the previous view casting can occur.
      ViewId proposed = this.proposed_view;
      if (vid != null && proposed != null && !proposed.equals(vid)) {
        return;
      }
        missing_acks.remove(member);
        received_acks.add(member);
        if(missing_acks.size() == 0)
            all_acks_received.setResult(Boolean.TRUE);
    }

    public void suspect(Object member) {
      ack(member, null);
      suspected_mbrs.add(member);
    }

    public void unsuspect(Object member) {
        suspected_mbrs.remove(member);
    }
    
    public Set getSuspectedMembers() { // GemStoneAddition
      return suspected_mbrs;
    }

    public void handleView(View v) {
        if(v == null) return;
        Vector mbrs=v.getMembers();
        suspected_mbrs.retainAll(mbrs);
    }

    public boolean waitForAllAcks() {
      synchronized(this) { // GemStoneAddition
        missing_acks.removeAll(suspected_mbrs); // GemStoneAddition - we don't wait for mbrs about to be kicked out
        if(missing_acks.size() == 0)
            return true;
      }
        Object result=all_acks_received.getResult();
        if(result != null && result instanceof Boolean && ((Boolean)result).booleanValue())
            return true;
        return false;
    }

    public boolean waitForAllAcks(long timeout) throws TimeoutException {
      synchronized(this) { // GemStoneAddition
        missing_acks.removeAll(suspected_mbrs); // GemStoneAddition - we don't wait for mbrs about to be kicked out
        if(missing_acks.size() == 0)
            return true;
      }
        Object result=all_acks_received.getResultWithTimeout(timeout);
        if(result != null && result instanceof Boolean && ((Boolean)result).booleanValue())
            return true;
        return false;
    }
    
    /** GemStoneAddition - returns a copy of the missing acks list */
    public synchronized java.util.List getMissingAcks() {
      return new ArrayList(this.missing_acks);
    }

    @Override // GemStoneAddition
    public String toString() {
        return "missing=" + missing_acks + ", received=" + received_acks;
    }
}
