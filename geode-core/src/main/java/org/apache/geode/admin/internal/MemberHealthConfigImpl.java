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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.MemberHealthConfig;

// @todo Make this class (and all of its subclasses) {@link java.io.Externalizable} or
// {@link org.apache.geode.DataSerializable}.
/**
 * The implementation of <code>MemberHealthConfig</code>
 *
 *
 * @since GemFire 3.5
 */
public abstract class MemberHealthConfigImpl implements MemberHealthConfig, java.io.Serializable {

  private static final long serialVersionUID = 3966032573073580490L;

  /**
   * The maximum process size (in megabytes) of a healthy member of the distributed system.
   */
  private long maxVMProcessSize = DEFAULT_MAX_VM_PROCESS_SIZE;

  /**
   * The maximum number of enqueued incoming or outgoing messages that a healthy member of a
   * distributed system can have.
   */
  private long maxMessageQueueSize = DEFAULT_MAX_MESSAGE_QUEUE_SIZE;

  /**
   * The maximum number message replies that can timeout in a healthy member.
   */
  private long maxReplyTimeouts = DEFAULT_MAX_REPLY_TIMEOUTS;

  /**
   * The maximum multicast retransmit / multicast message count ratio
   */
  private double maxRetransmissionRatio = DEFAULT_MAX_RETRANSMISSION_RATIO;


  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>MemberHealthConfigImpl</code> with the default configuration.
   */
  MemberHealthConfigImpl() {

  }

  ///////////////////// Instance Methods //////////////////////

  @Override
  public long getMaxVMProcessSize() {
    return maxVMProcessSize;
  }

  @Override
  public void setMaxVMProcessSize(long size) {
    maxVMProcessSize = size;
  }

  @Override
  public long getMaxMessageQueueSize() {
    return maxMessageQueueSize;
  }

  @Override
  public void setMaxMessageQueueSize(long maxMessageQueueSize) {
    this.maxMessageQueueSize = maxMessageQueueSize;
  }

  @Override
  public long getMaxReplyTimeouts() {
    return maxReplyTimeouts;
  }

  @Override
  public void setMaxReplyTimeouts(long maxReplyTimeouts) {
    this.maxReplyTimeouts = maxReplyTimeouts;
  }

  @Override
  public double getMaxRetransmissionRatio() {
    return maxRetransmissionRatio;
  }

  @Override
  public void setMaxRetransmissionRatio(double ratio) {
    maxRetransmissionRatio = ratio;
  }
}
