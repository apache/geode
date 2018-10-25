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

import org.apache.geode.admin.GemFireHealthConfig;

// @todo davidw Delegate to a "parent" config for properties that are not overridden.
// This will be made easier with a special <code>HealthConfigAttribute</code> class.
/**
 * The implementation of <code>GemFireHealthConfig</code>
 *
 *
 *
 * @since GemFire 3.5
 */
public class GemFireHealthConfigImpl extends CacheHealthConfigImpl implements GemFireHealthConfig {

  private static final long serialVersionUID = -6797673296902808018L;

  /** The name of the host to which this configuration applies. */
  private String hostName;

  /**
   * The number of seconds to wait between evaluating the health of GemFire.
   */
  private int interval = DEFAULT_HEALTH_EVALUATION_INTERVAL;

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>GemFireHealthConfigImpl</code> that applies to the host with the given
   * name.
   *
   * @param hostName The name of the host to which this configuration applies. If <code>null</code>,
   *        then this is the "default" configuration.
   */
  public GemFireHealthConfigImpl(String hostName) {
    this.hostName = hostName;
  }

  /////////////////////// Instance Methods ///////////////////////

  public String getHostName() {
    return this.hostName;
  }

  public void setHealthEvaluationInterval(int interval) {
    this.interval = interval;
  }

  public int getHealthEvaluationInterval() {
    return this.interval;
  }

  @Override
  public String toString() {
    if (this.hostName == null) {
      return "Default GemFire health configuration";

    } else {
      return String.format("GemFire health configuration for host %s",
          this.hostName);
    }
  }

}
