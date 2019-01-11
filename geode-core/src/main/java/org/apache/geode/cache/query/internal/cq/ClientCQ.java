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
package org.apache.geode.cache.query.internal.cq;

import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqException;

public interface ClientCQ extends InternalCqQuery {

  /**
   * Closes the Query. On Client side, sends the cq close request to server. On Server side, takes
   * care of repository cleanup.
   *
   * @param sendRequestToServer true to send the request to server.
   */
  @Override
  void close(boolean sendRequestToServer) throws CqClosedException, CqException;

  void setProxyCache(ProxyCache proxyCache);

  void createOn(Connection recoveredConnection, boolean isDurable);

}
