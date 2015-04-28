/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;

// @todo Make this class (and all of its subclasses) {@link java.io.Externalizable} or
// {@link com.gemstone.gemfire.DataSerializable}. 
/**
 * The implementation of <code>MemberHealthConfig</code>
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public abstract class MemberHealthConfigImpl
  implements MemberHealthConfig, java.io.Serializable {

  private static final long serialVersionUID = 3966032573073580490L;
  
  /** The maximum process size (in megabytes) of a healthy member of
   * the distributed system. */
  private long maxVMProcessSize = DEFAULT_MAX_VM_PROCESS_SIZE;

  /** The maximum number of enqueued incoming or outgoing
   * messages that a healthy member of a distributed system can
   * have. */
  private long maxMessageQueueSize = DEFAULT_MAX_MESSAGE_QUEUE_SIZE;

  /** The maximum number message replies that can timeout in a healthy
   * member. */
  private long maxReplyTimeouts = DEFAULT_MAX_REPLY_TIMEOUTS;

  /** The maximum multicast retransmit / multicast message count ratio
   */
  private double maxRetransmissionRatio = DEFAULT_MAX_RETRANSMISSION_RATIO;


  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>MemberHealthConfigImpl</code> with the
   * default configuration.
   */
  MemberHealthConfigImpl() {

  }

  /////////////////////  Instance Methods  //////////////////////

  public long getMaxVMProcessSize() {
    return this.maxVMProcessSize;
  }

  public void setMaxVMProcessSize(long size) {
    this.maxVMProcessSize = size;
  }

  public long getMaxMessageQueueSize() {
    return this.maxMessageQueueSize;
  }

  public void setMaxMessageQueueSize(long maxMessageQueueSize) {
    this.maxMessageQueueSize = maxMessageQueueSize;
  }

  public long getMaxReplyTimeouts() {
    return this.maxReplyTimeouts;
  }

  public void setMaxReplyTimeouts(long maxReplyTimeouts) {
    this.maxReplyTimeouts = maxReplyTimeouts;
  }

  public double getMaxRetransmissionRatio() {
    return this.maxRetransmissionRatio;
  }

  public void setMaxRetransmissionRatio(double ratio) {
    this.maxRetransmissionRatio = ratio;
  }
}
