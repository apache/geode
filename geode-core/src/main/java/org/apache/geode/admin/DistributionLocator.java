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

package org.apache.geode.admin;

/**
 * Represents a single distribution locator server, of which a
 * distributed system may use zero or many.  The distributed system
 * will be configured to use either multicast discovery or locator
 * service.
 *
 * @see DistributionLocatorConfig
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface DistributionLocator extends ManagedEntity {
  
  /** 
   * Returns the identity name for this locator.
   */
  public String getId();
  
  /**
   * Returns the configuration object for this distribution locator.
   *
   * @since GemFire 4.0
   */
  public DistributionLocatorConfig getConfig();

}

