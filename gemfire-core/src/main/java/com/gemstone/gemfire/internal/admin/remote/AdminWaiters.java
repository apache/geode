/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.OperationCancelledException;
import com.gemstone.gemfire.admin.RuntimeAdminException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * Used by {@link AdminRequest} to wait for a {@link AdminResponse}.
 * Prior to GemFire 4.0, a singleton instance of this would keep track
 * of <code>AdminRequest</code>s that were waiting for replies and
 * would cancel them, if desired.  However, in order to fix bug 31562,
 * <code>AdminRequest</code> were modified to use an {@link
 * AdminReplyProcessor} and this class was refactored to be more
 * static. 
 *
 * <P>
 *
 * Eventually, the waiting/cancelling code can be factored into
 * <code>AdminRequest</code> and this class can go away.
 */
public class AdminWaiters {
  private static final Logger logger = LogService.getLogger();
  
  //private static final long TIMEOUT = 10000L;

  /**
   * Sends <code>msg</code> using <code>dm</code> and waits for the response.
   * @return the response.
   * @throws RuntimeAdminException if this method is interrupted, times out, cancelled ({@link #cancelWaiters}), or failed with an exception on the server side.
   */
  public static AdminResponse sendAndWait(AdminRequest msg, DistributionManager dm) {

    // Prior to GemFire 4.0 admin messages were only sent to other
    // VMs; it was impossible for an admin message to be destined for
    // the VM that sent it.  However, now that the admin API can be
    // used in an application VM, the admin API may request
    // information about its own VM.  Unfortunately, a distribution
    // manager cannot send a message to itself (see bug 31734).  So,
    // we have to handle admin messages sent to ourselves specially.
    if (dm.getId().equals(msg.getRecipient())) {
      msg.setSender(dm.getId()); // Sent from myself
      return msg.createResponse(dm);
    }

    AdminResponse result = null;
    try {
      synchronized(msg) {
        Set failures = dm.putOutgoing(msg);
        if (failures != null && failures.size() > 0) { // didn't go out
          if (dm.getDistributionManagerIds().contains(msg.getRecipient())) {
            // it's still in the view
            String s = "";
            if (logger.isTraceEnabled(LogMarker.DM)) {
              s += " (" + msg + ")";
            }
            throw new RuntimeAdminException(LocalizedStrings.AdminWaiters_COULD_NOT_SEND_REQUEST_0.toLocalizedString(s));
          }
          throw new OperationCancelledException(LocalizedStrings.AdminWaiters_REQUEST_SENT_TO_0_FAILED_SINCE_MEMBER_DEPARTED_1.toLocalizedString(new Object[] {msg.getRecipient(), ""}));
        }
        // sent it
        
        long timeout = getWaitTimeout();
        boolean gotResponse = msg.waitForResponse(timeout);
        if (!gotResponse) {
          if (dm.isCurrentMember(msg.getRecipient())) { // still here?
            //no one ever replied
            StringBuffer sb =
              new StringBuffer("Administration request ");
            sb.append(msg);
            sb.append(" sent to ");
            sb.append(msg.getRecipient());
            sb.append(" timed out after ");
            sb.append((timeout / 1000));
            sb.append(" seconds.");

            throw new RuntimeAdminException(sb.toString());
          } // still here?
          // recipient vanished
          String s = "";
          if (logger.isTraceEnabled(LogMarker.DM)) {
            s = " (" + msg + ")";
          }
          throw new OperationCancelledException(LocalizedStrings.AdminWaiters_REQUEST_SENT_TO_0_FAILED_SINCE_MEMBER_DEPARTED_1.toLocalizedString(new Object[] {msg.getRecipient(), s}));
        } // !gotResponse
        
        result = msg.getResponse();
      } // synchronized
    } 
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      dm.getCancelCriterion().checkCancelInProgress(ex);
      String s = LocalizedStrings.AdminWaiters_REQUEST_WAIT_WAS_INTERRUPTED.toLocalizedString();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        s += " (" + msg + ")";
      }
      throw new RuntimeAdminException(s, ex);
    }

    if (result == null) {
      String s = "";
      if (logger.isTraceEnabled(LogMarker.DM)) {
        s += " (" + msg + ")";
      }
      throw new OperationCancelledException(LocalizedStrings.AdminWaiters_REQUEST_SEND_TO_0_WAS_CANCELLED_1.toLocalizedString(new Object[] {msg.getRecipient(), s}));

    } else if (result instanceof AdminFailureResponse) {
      throw new RuntimeAdminException(LocalizedStrings.AdminWaiters_REQUEST_FAILED.toLocalizedString(), ((AdminFailureResponse)result).getCause());
    }
    return result;
  }

  /**
   * Call to send a {@link AdminResponse} and notify the thread waiting for it.
   */
  public static void sendResponse(AdminResponse msg) {
    int id = msg.getMsgId();
    ReplyProcessor21 processor =
      (ReplyProcessor21) ReplyProcessor21.getProcessor(id);

    if (processor == null) {
      return; //must've been cancelled

    } else {
      processor.process(msg);
    }
  }
  /**
   * Call with the id of a RemoteGfManager that no longer exists.
   * All outstanding requests to that manager will be cancelled.
   */
  public static void cancelWaiters(InternalDistributedMember id) {
    // Now that AdminRequests use a ReplyProcessor, we don't need to
    // do anything here.  The ReplyProcessor takes care of members
    // that depart.
  }

  public static void cancelRequest(int msgId, DistributionManager dm) {
    AdminReplyProcessor processor =
      (AdminReplyProcessor) ReplyProcessor21.getProcessor(msgId);
    if (processor != null) {
      InternalDistributedMember recipient = processor.getResponder();
      dm.putOutgoing(CancellationMessage.create(recipient, msgId));
      processor.cancel();
    }    
  }

  private static long getWaitTimeout() {
    String prop = System.getProperty("remote.call.timeout", "1800");
    try {
      int val = Integer.parseInt(prop);
      return Math.abs(val * 1000L);
    } catch (NumberFormatException nfe) {
      return 1800 * 1000L;
    }
  }
  
}
