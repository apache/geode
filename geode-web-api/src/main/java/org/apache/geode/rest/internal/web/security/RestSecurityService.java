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
package org.apache.geode.rest.internal.web.security;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.GemFireSecurityException;
import org.springframework.stereotype.Component;

@Component("securityService")
public class RestSecurityService {

  private final SecurityService securityService;

  public RestSecurityService() {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      this.securityService = cache.getSecurityService();
    } else {
      this.securityService = SecurityServiceFactory.create();
    }
  }

  public boolean authorize(String resource, String operation) {
    return authorize(resource, operation, null, null);
  }

  public boolean authorize(String resource, String operation, String region) {
    return authorize(resource, operation, region, null);
  }

  public boolean authorize(String resource, String operation, String region, String key) {
    try {
      securityService.authorize(resource, operation, region, key);
      return true;
    } catch (GemFireSecurityException ex) {
      return false;
    }
  }

  public boolean authorize(String operation, String region, String[] keys) {
    boolean authorized = false;
    for (String key : keys) {
      authorized = authorize("DATA", operation, region, key);
      if (!authorized)
        return false;
    }
    return true;
  }

  public Object postProcess(String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return securityService.postProcess(regionPath, key, value, valueIsSerialized);
  }
}
