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

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * A <code>ReplyProcessor</code> that is used by an {@link AdminRequest} to wait for an
 * {@link AdminResponse}. If
 * {@link org.apache.geode.distributed.internal.ReplyProcessor21#waitForReplies()} returns saying
 * that we didn't time out and there is no response, it means that the member for which we were
 * awaiting a response has left the distributed system. This helps us fix bug 31562.
 *
 * <P>
 *
 * An <code>AdminReplyProcessor</code> can be {@linkplain #cancel cancelled}.
 *
 * @since GemFire 4.0
 */
class AdminReplyProcessor extends ReplyProcessor21 {

  /** The response to the <code>AdminRequest</code>. */
  private volatile AdminResponse response;

  /**
   * The thread that is waiting for replies. We need this thread so that we can interrupt it when
   * this processor is cancelled.
   */
  private volatile Thread thread;

  /** Has this reply processor been cancelled? */
  private volatile boolean isCancelled;

  /** The member from whom we expect a response */
  private final InternalDistributedMember responder;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>AdminReplyProcessor</code> that waits for a reply from the given member of
   * the distributed system. Note that <code>AdminRequest</code>s are only sent to one member of the
   * distributed system.
   */
  AdminReplyProcessor(InternalDistributedSystem system, InternalDistributedMember member) {
    super(system, member);
    this.isCancelled = false;
    this.responder = member;
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Keep track of the <code>AdminResponse</code> we received.
   */
  @Override
  public void process(DistributionMessage message) {
    try {
      this.response = (AdminResponse) message;

    } finally {
      super.process(message);
    }
  }

  /**
   * Since we can't override <code>waitForReplies</code>, we have to get the thread that is waiting
   * in <code>preWait</code>.
   */
  @Override
  protected void preWait() {
    this.thread = Thread.currentThread();
    super.preWait();
  }

  /**
   * "Cancels" this reply processor by instructing the thread that is waiting for replies that it
   * shouldn't wait any more.
   */
  public void cancel() {
    this.isCancelled = true;
    if (this.thread != null) {
      this.thread.interrupt();
    }
  }

  /**
   * If we are interrupted after we are cancelled, return <code>true</code> indicating that there is
   * a "response". That way, we won't complain about timing out. However, the response is still
   * <code>null</code>.
   *
   * <P>
   *
   * Otherwise, re-throw the <code>InterruptedException</code>.
   */
  protected boolean handleInterruption(InterruptedException ie, long msecsRemaining)
      throws InterruptedException, ReplyException {

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (this.isCancelled) {
      return true;

    } else {
      throw ie;
    }
  }

  /**
   * Returns the response that this processor has been waiting for. If this method returns
   * <code>null</code> and <code>waitForReplies</code> return <code>true</code>, it means that the
   * request was cancelled. If this method returns <code>null</code> and <code>waitForReplies</code>
   * return <code>false</code>, it means that <code>waitForReplies</code> timed out.
   */
  public AdminResponse getResponse() {
    return this.response;
  }

  /**
   * Returns the member who we are waiting to send us a response.
   */
  public InternalDistributedMember getResponder() {
    return this.responder;
  }

}
