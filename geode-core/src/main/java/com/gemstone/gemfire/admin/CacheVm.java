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
 * A dedicated cache server VM that is managed by the administration
 * API.
 * <p>Note that this may not represent an instance of
 * {@link com.gemstone.gemfire.cache.server.CacheServer}. It is possible for
 * a cache VM to be started but for it not to listen for client connections
 * in which case it is not a 
 * {@link com.gemstone.gemfire.cache.server.CacheServer}
 * but is an instance of this interface.
 *
 * @since GemFire 5.7
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface CacheVm extends SystemMember, ManagedEntity {
  /**
   * Returns the configuration of this cache vm
   */
  public CacheVmConfig getVmConfig();
}
