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
package org.apache.geode.admin;

/**
 * An administration alert that is issued by a member of a GemFire distributed system. It is similar
 * to a {@linkplain org.apache.geode.LogWriter log message}.
 *
 * @see AlertListener
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public interface Alert {

  /** The level at which this alert is issued */
  AlertLevel getLevel();

  /**
   * The member of the distributed system that issued the alert, or null if the issuer is no longer
   * a member of the distributed system.
   */
  SystemMember getSystemMember();

  /**
   * The name of the {@linkplain org.apache.geode.distributed.DistributedSystem#getName distributed
   * system}) through which the alert was issued.
   */
  String getConnectionName();

  /** The id of the source of the alert (such as a thread in a VM) */
  String getSourceId();

  /** The alert's message */
  String getMessage();

  /** The time at which the alert was issued */
  java.util.Date getDate();

}
