/*
 *
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
 *
 */

package org.apache.geode.security;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * IntegratedSecurityService extension that allows for manipulation of behavior for testing
 * handling of rare occurrences of responses of the security service in a more predictable way.
 */
public class TestIntegratedSecurityService extends IntegratedSecurityService {
  private static final Logger logger = LogService.getLogger();

  public static final String FAIL_INTERVAL = "fail_interval";
  public static final String CALL_COUNT = "call_count";
  public static final String FAIL_TIMES = "fail_times";

  private final AtomicInteger getSubjectFailInterval = new AtomicInteger(0);
  private final AtomicInteger getSubjectCallCount = new AtomicInteger(0);
  private final AtomicInteger getSubjectTimesFailed = new AtomicInteger(0);

  public TestIntegratedSecurityService() {
    super(new SecurityManagerProvider(new ExpirableSecurityManager()), null);
  }

  @Override
  protected Subject getCurrentUser() {
    Subject subject = super.getCurrentUser();
    if (getSubjectCallCount.get() > getSubjectFailInterval.get()) {
      getSubjectTimesFailed.incrementAndGet();
      logger.info("about to throw induced exception on call count: {}", getSubjectCallCount.get());
      getSubjectCallCount.set(0);
      throw new UnavailableSecurityManagerException("SecurityManager unavailable");
    }

    getSubjectCallCount.incrementAndGet();
    return subject;
  }

  public void reset() {
    getSubjectCallCount.set(0);
    getSubjectFailInterval.set(0);
    getSubjectTimesFailed.set(0);
  }

  public void setFailInterval(int failInterval) {
    getSubjectFailInterval.set(failInterval);
  }

  public Map<String, Object> getGetSubjectFailInformation() {
    Map<String, Object> infoMap = new HashMap<>();
    infoMap.put(CALL_COUNT, getSubjectCallCount.get());
    infoMap.put(FAIL_INTERVAL, getSubjectFailInterval.get());
    infoMap.put(FAIL_TIMES, getSubjectTimesFailed.get());
    return infoMap;
  }
}
