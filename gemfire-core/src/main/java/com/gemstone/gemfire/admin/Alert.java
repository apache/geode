/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * An administration alert that is issued by a member of a GemFire
 * distributed system.  It is similar to a {@linkplain
 * com.gemstone.gemfire.i18n.LogWriterI18n log message}.
 *
 * @author    Kirk Lund
 * @see       AlertListener
 * @since     3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface Alert {
  
  /** The level at which this alert is issued */
  public AlertLevel getLevel();

  /**
   * The member of the distributed system that issued the alert, or
   * null if the issuer is no longer a member of the distributed system.
   */
  public SystemMember getSystemMember();

  /** 
   * The name of the {@linkplain
   * com.gemstone.gemfire.distributed.DistributedSystem#getName
   * distributed system}) through which the alert was issued.
   */
  public String getConnectionName();

  /** The id of the source of the alert (such as a thread in a VM) */
  public String getSourceId();

  /** The alert's message */
  public String getMessage();

  /** The time at which the alert was issued */
  public java.util.Date getDate();

}
