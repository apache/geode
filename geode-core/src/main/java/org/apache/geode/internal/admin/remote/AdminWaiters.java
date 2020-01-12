/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.geode.internal.admin.remote;

import java.util.Set;

import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Used by {@link AdminRequest} to wait for a {@link AdminResponse}. Prior to GemFire 4.0, a
 * singleton instance of this would keep track of <code>AdminRequest</code>s that were waiting for
 * replies and would cancel them, if desired. However, in order to fix bug 31562,
 * <code>AdminRequest</code> were modified to use an {@link AdminReplyProcessor} and this class was
 * refactored to be more static.
 *
 * <P>
 *
 * Eventually, the waiting/cancelling code can be factored into <code>AdminRequest</code> and this
 * class can go away.
 */
public class AdminWaiters {
  /**
   * Sends <code>msg</code> using <code>dm</code> and waits for the response.
   *
   * @return the response.
   * @throws RuntimeAdminException if this method is interrupted, times out, cancelled
   *         ({@link #cancelWaiters}), or failed with an exception on the server side.
   */
  public static AdminResponse sendAndWait(AdminRequest msg, ClusterDistributionManager dm) {

    // Prior to GemFire 4.0 admin messages were only sent to other
    // VMs; it was impossible for an admin message to be destined for
    // the VM that sent it. However, now that the admin API can be
    // used in an application VM, the admin API may request
    // information about its own VM. Unfortunately, a distribution
    // manager cannot send a message to itself (see bug 31734). So,
    // we have to handle admin messages sent to ourselves specially.
    if (dm.getId().equals(msg.getRecipient())) {
      msg.setSender(dm.getId()); // Sent from myself
      return msg.createResponse(dm);
    }

    AdminResponse result = null;
    try {
      synchronized (msg) {
        Set failures = dm.putOutgoing(msg);
        if (failures != null && failures.size() > 0) { // didn't go out
          if (dm.getDistributionManagerIds().contains(msg.getRecipient())) {
            // it's still in the view
            String s = " (" + msg + ")";
            throw new RuntimeAdminException(
                String.format("Could not send request.%s", s));
          }
          throw new OperationCancelledException(
              String.format("Request sent to %s failed since member departed.%s",
                  new Object[] {msg.getRecipient(), ""}));
        }
        // sent it

        long timeout = getWaitTimeout();
        boolean gotResponse = msg.waitForResponse(timeout);
        if (!gotResponse) {
          if (dm.isCurrentMember(msg.getRecipient())) { // still here?
            // no one ever replied
            StringBuffer sb = new StringBuffer("Administration request ");
            sb.append(msg);
            sb.append(" sent to ");
            sb.append(msg.getRecipient());
            sb.append(" timed out after ");
            sb.append((timeout / 1000));
            sb.append(" seconds.");

            throw new RuntimeAdminException(sb.toString());
          } // still here?
          // recipient vanished
          String s = " (" + msg + ")";
          throw new OperationCancelledException(
              String.format("Request sent to %s failed since member departed.%s",
                  new Object[] {msg.getRecipient(), s}));
        } // !gotResponse

        result = msg.getResponse();
      } // synchronized
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      dm.getCancelCriterion().checkCancelInProgress(ex);
      String s = "Request wait was interrupted.";
      s += " (" + msg + ")";
      throw new RuntimeAdminException(s, ex);
    }

    if (result == null) {
      String s = " (" + msg + ")";
      throw new OperationCancelledException(
          String.format("Request sent to %s was cancelled. %s",
              msg.getRecipient(), s));

    } else if (result instanceof AdminFailureResponse) {
      throw new RuntimeAdminException(
          "Request failed.",
          ((AdminFailureResponse) result).getCause());
    }
    return result;
  }

  /**
   * Call to send a {@link AdminResponse} and notify the thread waiting for it.
   */
  public static void sendResponse(AdminResponse msg) {
    int id = msg.getMsgId();
    ReplyProcessor21 processor = (ReplyProcessor21) ReplyProcessor21.getProcessor(id);

    if (processor == null) {
      return; // must've been cancelled
    } else {
      processor.process(msg);
    }
  }

  /**
   * Call with the id of a RemoteGfManager that no longer exists. All outstanding requests to that
   * manager will be cancelled.
   */
  public static void cancelWaiters(InternalDistributedMember id) {
    // Now that AdminRequests use a ReplyProcessor, we don't need to
    // do anything here. The ReplyProcessor takes care of members
    // that depart.
  }

  public static void cancelRequest(int msgId, ClusterDistributionManager dm) {
    AdminReplyProcessor processor = (AdminReplyProcessor) ReplyProcessor21.getProcessor(msgId);
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
