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

package org.apache.geode.security;

import java.util.Properties;

/**
 * PostProcessor allows the customer to massage the values seen by a particular user.
 *
 * @since Geode 1.0
 */
public interface PostProcessor {

  /**
   * Given the security props of the server, properly initialize the post processor for the server.
   * Initialized at cache creation
   * 
   * @param securityProps security properties
   */
  default void init(Properties securityProps) {}

  /**
   * Process the value before sending it to the requester
   *
   * @param principal The principal that's accessing the value. The type of the principal will
   *        depend on how you implemented your SecurityManager
   * @param regionName The region that's been accessed. This could be null.
   * @param key the key of the value that's been accessed. This could be null.
   * @param value the original value. The original value could be null as well.
   * @return the value that will be returned to the requester
   */
  Object processRegionValue(Object principal, String regionName, Object key, Object value);

  /**
   * Give the implementation a chance to close the resources used. Called when cache is closed.
   */
  default void close() {}
}
