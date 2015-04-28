/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This is a reply processor which tracks departed members in order for
 * reliable messaging to determine which recipients departed before replying.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class ReliableReplyProcessor21 extends ReplyProcessor21 {

  /** The members that departed before replying */
  private Set departedMembers;

  public ReliableReplyProcessor21(InternalDistributedSystem system, InternalDistributedMember member) {
    super(system, member);
  }
  public ReliableReplyProcessor21(DM dm, InternalDistributedMember member) {
    super(dm, member);
  }
  public ReliableReplyProcessor21(DM dm, Collection initMembers) {
    super(dm, initMembers);
  }
  public ReliableReplyProcessor21(InternalDistributedSystem system, Collection initMembers) {
    super(system, initMembers);
  }
  
  /**
   * This method is invoked after a member has explicitly left
   * the system.  It may not get invoked if a member becomes unreachable
   * due to crash or network problems.
   * <p>
   * ReliableReplyProcessor21 overrides this to add the departed member to
   * the departedMembers if we haven't already received a reply from that
   * member.
   * <p>
   * Note: race condition exists between membershipListener and processing 
   * of replies.
   */
  @Override
  public void memberDeparted(final InternalDistributedMember id, final boolean crashed) {
    if (removeMember(id, true)) {
      synchronized (this) {
        if (this.departedMembers == null) {
          this.departedMembers = new HashSet();
        }
        this.departedMembers.add(id);
      }
    }
    checkIfDone();
  }
  
  /**
   * Returns the recipients that have departed prior to processing a reply
   * from them.
   */
  public Set getDepartedMembers() {
    synchronized(this) {
      if (this.departedMembers == null) {
        return Collections.EMPTY_SET;
      } else {
        return this.departedMembers;
      }
    }
  }
  
  /**
   * Use this method instead of {@link #waitForReplies()} if you want the wait to throw an
   * exception when a member departs.
   * @throws ReplyException the exception passed back in reply
   * @throws InterruptedException
   * @throws ReliableReplyException when a member departs
   */
  public final void waitForReliableDelivery() throws ReplyException, InterruptedException, ReliableReplyException {
    waitForReliableDelivery(0);
  }
  
  /**
   * @see #waitForReliableDelivery()
   * @param msecs the number of milliseconds to wait for replies
   * @throws ReplyException
   * @throws InterruptedException
   * @throws ReliableReplyException
   */
  public final void waitForReliableDelivery(long msecs) throws ReplyException, InterruptedException, ReliableReplyException {
    super.waitForReplies(msecs);
    synchronized (this) {
      if (this.departedMembers != null) {
        throw new ReliableReplyException(LocalizedStrings.ReliableReplyProcessor_FAILED_TO_DELIVER_MESSAGE_TO_MEMBERS_0
            .toLocalizedString(departedMembers));
      }
    }
  }
}

