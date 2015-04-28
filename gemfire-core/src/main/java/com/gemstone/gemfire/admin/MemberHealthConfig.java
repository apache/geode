/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to the health of a
 * member of a GemFire distributed system.
 *
 * <P>
 *
 * If any of the following criteria is true, then a member is
 * considered to be in {@link GemFireHealth#OKAY_HEALTH OKAY_HEALTH}.
 *
 * <UL>
 *
 * <LI>The size of the {@linkplain #getMaxVMProcessSize VM process} is
 * too large.</LI>
 *
 * <LI>There are too many {@linkplain #getMaxMessageQueueSize enqueued}
 * incoming/outgoing messages.</LI>
 *
 * <LI>Too many message sends {@link #getMaxReplyTimeouts timeout}
 * while waiting for a reply.</LI>
 *
 * </UL>
 *
 * If any of the following criteria is true, then a member is
 * considered to be in {@link GemFireHealth#POOR_HEALTH POOR_HEALTH}.
 *
 * <UL>
 *
 * </UL>
 *
 * @author David Whitlock
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 * */
public interface MemberHealthConfig {

  /** The default maximum VM process size (in megabytes) of a health
   * member of the distributed system. The default value is 1000. */
  public static final long DEFAULT_MAX_VM_PROCESS_SIZE = 1000;

  /** The default maximum number of enqueued incoming or outgoing
   * messages that a healthy member of a distributed system can have.
   * The default value is 1000. */
  public static final long DEFAULT_MAX_MESSAGE_QUEUE_SIZE = 1000;

  /** The default maximum number of message reply timeouts that can
   * occur in a given health monitoring interval. The default value
   * is zero. */
  public static final long DEFAULT_MAX_REPLY_TIMEOUTS = 0;

  /** The default maximum multicast retransmission ratio.  The default
   * value is 0.20 (twenty percent of messages retransmitted)
   */
  public static final double DEFAULT_MAX_RETRANSMISSION_RATIO = 0.20;
  
  ///////////////////////  Instance Methods  ///////////////////////

  /**
   * Returns the maximum VM process size (in megabytes) of a healthy
   * member of the distributed system.
   *
   * @see #DEFAULT_MAX_VM_PROCESS_SIZE
   */
  public long getMaxVMProcessSize();

  /**
   * Sets the maximum VM process size (in megabytes) of a healthy
   * member of the distributed system.
   *
   * @see #getMaxVMProcessSize
   */
  public void setMaxVMProcessSize(long size);
  
  /**
   * Returns the maximum number of enqueued incoming or outgoing
   * messages that a healthy member of a distributed system can have.
   *
   * @see #DEFAULT_MAX_MESSAGE_QUEUE_SIZE
   */
  public long getMaxMessageQueueSize();

  /**
   * Sets the maximum number of enqueued incoming or outgoing
   * messages that a healthy member of a distributed system can have.
   *
   * @see #getMaxMessageQueueSize
   */
  public void setMaxMessageQueueSize(long maxMessageQueueSize);

  /**
   * Returns the maximum number message replies that can timeout in a
   * healthy member.
   *
   * @see #DEFAULT_MAX_REPLY_TIMEOUTS
   */
  public long getMaxReplyTimeouts();

  /**
   * Sets the maximum number message replies that can timeout in a
   * healthy member.
   *
   * @see #getMaxReplyTimeouts
   */
  public void setMaxReplyTimeouts(long maxReplyTimeouts);

  /**
   * Returns the maximum ratio of multicast retransmissions / total multicast
   * messages.  Retransmissions are requestor-specific (i.e., unicast), so
   * a single lost message may result in multiple retransmissions.<p>
   * A high retransmission ratio may indicate
   * poor network conditions requiring reduced flow-control settings,
   * a udp-fragment-size setting that is too high.
   * @see #DEFAULT_MAX_RETRANSMISSION_RATIO
   */
  public double getMaxRetransmissionRatio();
  
  /**
   * Sets the maximum ratio of multicast retransmissions / total multicast
   * messages.
   * @see #getMaxRetransmissionRatio
   */
  public void setMaxRetransmissionRatio(double ratio);
   
}
