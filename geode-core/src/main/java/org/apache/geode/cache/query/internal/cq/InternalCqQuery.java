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
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.internal.cache.LocalRegion;

public interface InternalCqQuery extends CqQuery {

  /**
   * sets the CqName.
   */
  public abstract void setName(String cqName);

  public String getName();

  /**
   * Closes the Query. On Client side, sends the cq close request to server. On Server side, takes
   * care of repository cleanup.
   * 
   * @param sendRequestToServer true to send the request to server.
   * @throws CqException
   */
  public abstract void close(boolean sendRequestToServer) throws CqClosedException, CqException;


  public abstract LocalRegion getCqBaseRegion();

  /**
   * @return Returns the serverCqName.
   */
  public abstract String getServerCqName();

  /**
   * Sets the state of the cq. Server side method. Called during cq registration time.
   */
  public abstract void setCqState(int state);

  /**
   * get the region name in CQ query
   */
  public String getRegionName();

  public abstract void setCqService(CqService cqService);

  /**
   * Returns a reference to VSD stats of the CQ
   * 
   * @return VSD stats of the CQ
   */
  public CqQueryVsdStats getVsdStats();

  public abstract CqState getState();

  public String getQueryString();

  public abstract boolean isDurable();

  public abstract void close() throws CqClosedException, CqException;

  public abstract void stop() throws CqClosedException, CqException;
}
