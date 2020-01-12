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

package org.apache.geode.distributed.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * This is a reply processor which tracks departed members in order for reliable messaging to
 * determine which recipients departed before replying.
 *
 * @since GemFire 5.0
 */
public class ReliableReplyProcessor21 extends ReplyProcessor21 {

  /** The members that departed before replying */
  private Set departedMembers;

  public ReliableReplyProcessor21(InternalDistributedSystem system,
      InternalDistributedMember member) {
    super(system, member);
  }

  public ReliableReplyProcessor21(DistributionManager dm, InternalDistributedMember member) {
    super(dm, member);
  }

  public ReliableReplyProcessor21(DistributionManager dm, Collection initMembers) {
    super(dm, initMembers);
  }

  public ReliableReplyProcessor21(InternalDistributedSystem system, Collection initMembers) {
    super(system, initMembers);
  }

  /**
   * This method is invoked after a member has explicitly left the system. It may not get invoked if
   * a member becomes unreachable due to crash or network problems.
   * <p>
   * ReliableReplyProcessor21 overrides this to add the departed member to the departedMembers if we
   * haven't already received a reply from that member.
   * <p>
   * Note: race condition exists between membershipListener and processing of replies.
   */
  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {
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
   * Returns the recipients that have departed prior to processing a reply from them.
   */
  public Set getDepartedMembers() {
    synchronized (this) {
      if (this.departedMembers == null) {
        return Collections.EMPTY_SET;
      } else {
        return this.departedMembers;
      }
    }
  }

  /**
   * Use this method instead of {@link #waitForReplies()} if you want the wait to throw an exception
   * when a member departs.
   *
   * @throws ReplyException the exception passed back in reply
   * @throws ReliableReplyException when a member departs
   */
  public void waitForReliableDelivery()
      throws ReplyException, InterruptedException, ReliableReplyException {
    waitForReliableDelivery(0);
  }

  /**
   * @see #waitForReliableDelivery()
   * @param msecs the number of milliseconds to wait for replies
   */
  public void waitForReliableDelivery(long msecs)
      throws ReplyException, InterruptedException, ReliableReplyException {
    super.waitForReplies(msecs);
    synchronized (this) {
      if (this.departedMembers != null) {
        throw new ReliableReplyException(
            String.format("Failed to deliver message to members: %s",
                departedMembers));
      }
    }
  }
}
