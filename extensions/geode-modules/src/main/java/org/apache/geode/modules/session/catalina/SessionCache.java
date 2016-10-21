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
package org.apache.geode.modules.session.catalina;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;
import org.apache.catalina.Session;

import javax.servlet.http.HttpSession;
import java.util.Set;

public interface SessionCache {

  public void initialize();

  public String getDefaultRegionAttributesId();

  public boolean getDefaultEnableLocalCache();

  public String getSessionRegionName();

  public String getOperatingRegionName();

  public void putSession(Session session);

  public HttpSession getSession(String sessionId);

  public void destroySession(String sessionId);

  public void touchSessions(Set<String> sessionIds);

  public DeltaSessionStatistics getStatistics();

  public GemFireCache getCache();

  public Region<String, HttpSession> getSessionRegion();

  public Region<String, HttpSession> getOperatingRegion();

  public boolean isPeerToPeer();

  public boolean isClientServer();

  public Set<String> keySet();

  public int size();

  public boolean isBackingCacheAvailable();
}
