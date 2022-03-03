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

import org.apache.geode.admin.DistributedSystemHealthConfig;

/**
 * The implementation of <code>DistributedSystemHealthConfig</code>. Note that because it never
 * leaves the management VM, it is not <code>Serializable</code> and is not part of the
 * {@link GemFireHealthConfigImpl} class hierarchy.
 *
 *
 * @since GemFire 3.5
 */
public class DistributedSystemHealthConfigImpl implements DistributedSystemHealthConfig {

  /**
   * The maximum number of application members that can unexceptedly leave a healthy the distributed
   * system.
   */
  private long maxDepartedApplications = DEFAULT_MAX_DEPARTED_APPLICATIONS;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthConfigImpl</code> with the default configuration.
   */
  protected DistributedSystemHealthConfigImpl() {

  }

  ///////////////////// Instance Methods /////////////////////

  @Override
  public long getMaxDepartedApplications() {
    return maxDepartedApplications;
  }

  @Override
  public void setMaxDepartedApplications(long maxDepartedApplications) {
    this.maxDepartedApplications = maxDepartedApplications;
  }
}
