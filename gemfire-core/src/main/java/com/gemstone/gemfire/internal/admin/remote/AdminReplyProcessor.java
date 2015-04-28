/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A <code>ReplyProcessor</code> that is used by an {@link
 * AdminRequest} to wait for an {@link AdminResponse}.  If 
 * {@link com.gemstone.gemfire.distributed.internal.ReplyProcessor21#waitForReplies()} 
 * returns saying that we didn't time out and there
 * is no response, it means that the member for which we were awaiting
 * a response has left the distributed system.  This helps us fix bug
 * 31562.
 *
 * <P>
 *
 * An <code>AdminReplyProcessor</code> can be {@linkplain #cancel
 * cancelled}.  
 *
 * @author David Whitlock
 * @since 4.0
 */
class AdminReplyProcessor extends ReplyProcessor21 {

  /** The response to the <code>AdminRequest</code>. */
  private volatile AdminResponse response;

  /** The thread that is waiting for replies.  We need this thread so
   * that we can interrupt it when this processor is cancelled. */
  private volatile Thread thread;

  /** Has this reply processor been cancelled? */
  private volatile boolean isCancelled;

  /** The member from whom we expect a response */
  private final InternalDistributedMember responder;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>AdminReplyProcessor</code> that waits for a
   * reply from the given member of the distributed system.  Note that
   * <code>AdminRequest</code>s are only sent to one member of the
   * distributed system.
   */
  AdminReplyProcessor(InternalDistributedSystem system,
                      InternalDistributedMember member) {
    super(system, member);
    this.isCancelled = false;
    this.responder = member;
  }

  //////////////////////  Instance Methods  //////////////////////

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
   * Since we can't override <code>waitForReplies</code>, we have to
   * get the thread that is waiting in <code>preWait</code>.
   */
  @Override
  protected void preWait() {
    this.thread = Thread.currentThread();
    super.preWait();
  }

  /**
   * "Cancels" this reply processor by instructing the thread that is
   * waiting for replies that it shouldn't wait any more.
   */
  public void cancel() {
    this.isCancelled = true;
    if (this.thread != null) {
      this.thread.interrupt();
    }
  }

  /**
   * If we are interrupted after we are cancelled, return
   * <code>true</code> indicating that there is a "response".  That
   * way, we won't complain about timing out.  However, the response
   * is still <code>null</code>.
   *
   * <P>
   *
   * Otherwise, re-throw the <code>InterruptedException</code>.
   */
  protected boolean handleInterruption(InterruptedException ie,
                                       long msecsRemaining)
    throws InterruptedException, ReplyException {

    if (Thread.interrupted()) throw new InterruptedException();
    if (this.isCancelled) {
      return true;

    } else {
      throw ie;
    }
  }

  /**
   * Returns the response that this processor has been waiting for.
   * If this method returns <code>null</code> and
   * <code>waitForReplies</code> return <code>true</code>, it means
   * that the request was cancelled.  If this method returns
   * <code>null</code> and <code>waitForReplies</code> return
   * <code>false</code>, it means that <code>waitForReplies</code>
   * timed out.
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
