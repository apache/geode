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
package org.apache.geode.cache;

/**
 * Indicates that a {@link Region} reliability failure has occurred. Reliability for a
 * <code>Region</code> is defined by its {@link MembershipAttributes}.
 *
 * @deprecated this feature is scheduled to be removed
 */
public abstract class RegionRoleException extends RoleException {
  private static final long serialVersionUID = 202660222740132890L;

  /** The full path of the region affected by the reliability failure */
  private final String regionFullPath;

  /**
   * Constructs a <code>RegionRoleException</code> with a message.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   */
  public RegionRoleException(String s, String regionFullPath) {
    super(s);
    this.regionFullPath = regionFullPath;
  }

  /**
   * Constructs a <code>RegionRoleException</code> with a message and a cause.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param ex the Throwable cause
   */
  public RegionRoleException(String s, String regionFullPath, Throwable ex) {
    super(s, ex);
    this.regionFullPath = regionFullPath;
  }

  /**
   * Returns the full path of the region for which access was attempted.
   *
   * @return the full path of the region for which access was attempted
   */
  public String getRegionFullPath() {
    return regionFullPath;
  }

}
