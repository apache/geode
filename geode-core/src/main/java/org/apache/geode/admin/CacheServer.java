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
 * A dedicated cache server VM that is managed by the administration API.
 *
 * @since GemFire 4.0
 * @deprecated as of 5.7 use {@link CacheVm} instead.
 */
@Deprecated
public interface CacheServer extends SystemMember, ManagedEntity {
  /**
   * Returns the configuration of this cache vm
   * 
   * @deprecated as of 5.7 use {@link CacheVm#getVmConfig} instead.
   */
  @Deprecated
  public CacheServerConfig getConfig();

  /**
   * Find whether this server is primary for given client (durableClientId)
   * 
   * @param durableClientId - durable-id of the client
   * @return true if the server is primary for given client
   * 
   * @since GemFire 5.6
   */
  public boolean isPrimaryForDurableClient(String durableClientId);

}
