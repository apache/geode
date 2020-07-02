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

package org.apache.geode.cache.client.internal;

/**
 * An operation to perform on a server. Used by {@link ExecutablePool} to attempt the operation on
 * multiple servers until the retryAttempts is exceeded.
 *
 * @since GemFire 5.7
 *
 */
public interface Op {

  /**
   * Attempts to execute this operation by sending its message out on the given connection, waiting
   * for a response, and returning it.
   *
   * @param cnx the connection to use when attempting execution of the operation.
   * @return the result of the operation or <code>null</code if the operation has no result.
   * @throws Exception if the execute failed
   */
  Object attempt(ClientCacheConnection cnx) throws Exception;

}
