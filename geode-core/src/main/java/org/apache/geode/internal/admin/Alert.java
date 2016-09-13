/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.admin;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.InternalLogWriter;

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
   * @since GemFire 6.5
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
