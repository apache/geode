/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * An administration alert that is issued by a member of a GemFire
 * distributed system.  It is similar to a log message.
 *
 * @see AlertListener
 */
public interface Alert {
  /** The level at which this alert is issued */
  public int getLevel();

  /** The member of the distributed system that issued the alert */
  public GemFireVM getGemFireVM();

  /** The name of the <code>GemFireConnection</code> through which the
   * alert was issued. */
  public String getConnectionName();

  /** The id of the source of the alert (such as a thread in a VM) */
  public String getSourceId();

  /** The alert's message */
  public String getMessage();

  /** The time at which the alert was issued */
  public java.util.Date getDate();

  /**
   * Returns a InternalDistributedMember instance representing a member that is
   * sending (or has sent) this alert. Could be <code>null</code>.
   * 
   * @return the InternalDistributedMember instance representing a member that
   *         is sending/has sent this alert
   *
   * @since 6.5        
   */
  public InternalDistributedMember getSender();

  public final static int ALL = InternalLogWriter.ALL_LEVEL;
  public final static int OFF = InternalLogWriter.NONE_LEVEL;
  public final static int FINEST = InternalLogWriter.FINEST_LEVEL;
  public final static int FINER = InternalLogWriter.FINER_LEVEL;
  public final static int FINE = InternalLogWriter.FINE_LEVEL;
  public final static int CONFIG = InternalLogWriter.CONFIG_LEVEL;
  public final static int INFO = InternalLogWriter.INFO_LEVEL;
  public final static int WARNING = InternalLogWriter.WARNING_LEVEL;
  public final static int ERROR = InternalLogWriter.ERROR_LEVEL;
  public final static int SEVERE = InternalLogWriter.SEVERE_LEVEL;
}
