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
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to all of the
 * components of a GemFire distributed system.
 *
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 * */
public interface GemFireHealthConfig
  extends MemberHealthConfig, CacheHealthConfig {

  /** The default number of seconds between assessments of the health
   * of the GemFire components. */
  public static final int DEFAULT_HEALTH_EVALUATION_INTERVAL = 30;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the name of the host to which this configuration
   * applies.  If this is the "default" configuration, then
   * <code>null</code> is returned.
   *
   * @see GemFireHealth#getGemFireHealthConfig
   */
  public String getHostName();

  /**
   * Sets the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public void setHealthEvaluationInterval(int interval);

  /**
   * Returns the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public int getHealthEvaluationInterval();

}
