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

import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqState;
import org.apache.geode.cache.query.internal.CqQueryVsdStats;
import org.apache.geode.internal.cache.LocalRegion;

public interface InternalCqQuery extends CqQuery {

  /**
   * sets the CqName.
   */
  void setName(String cqName);

  @Override
  String getName();

  /**
   * Closes the Query. On Client side, sends the cq close request to server. On Server side, takes
   * care of repository cleanup.
   *
   * @param sendRequestToServer true to send the request to server.
   */
  void close(boolean sendRequestToServer) throws CqClosedException, CqException;


  LocalRegion getCqBaseRegion();

  /**
   * @return Returns the serverCqName.
   */
  String getServerCqName();

  /**
   * Sets the state of the cq. Server side method. Called during cq registration time.
   */
  void setCqState(int state);

  /**
   * get the region name in CQ query
   */
  String getRegionName();

  void setCqService(CqService cqService);

  /**
   * Returns a reference to VSD stats of the CQ
   *
   * @return VSD stats of the CQ
   */
  CqQueryVsdStats getVsdStats();

  @Override
  CqState getState();

  @Override
  String getQueryString();

  @Override
  boolean isDurable();

  @Override
  void close() throws CqClosedException, CqException;

  @Override
  void stop() throws CqClosedException, CqException;

  @Override
  boolean isUpdateSuppressed();
}
