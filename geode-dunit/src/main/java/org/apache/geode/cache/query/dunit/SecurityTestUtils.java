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

package org.apache.geode.cache.query.dunit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.security.ExpirableSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class SecurityTestUtils {
  public static ExpirableSecurityManager collectSecurityManagers(MemberVM... vms) {
    List<ExpirableSecurityManager> results = new ArrayList<>();
    for (MemberVM vm : vms) {
      results.add(vm.invoke(() -> getSecurityManager()));
    }

    ExpirableSecurityManager consolidated = new ExpirableSecurityManager();
    for (ExpirableSecurityManager result : results) {
      consolidated.getExpiredUsers().addAll(result.getExpiredUsers());
      combine(consolidated.getAuthorizedOps(), result.getAuthorizedOps());
      combine(consolidated.getUnAuthorizedOps(), result.getUnAuthorizedOps());
    }
    return consolidated;
  }

  public static void combine(Map<String, List<String>> to, Map<String, List<String>> from) {
    for (String key : from.keySet()) {
      if (to.containsKey(key)) {
        to.get(key).addAll(from.get(key));
      } else {
        to.put(key, from.get(key));
      }
    }
  }

  public static ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) Objects.requireNonNull(ClusterStartupRule.getCache())
        .getSecurityService()
        .getSecurityManager();
  }

  public static class EventsCqListner implements CqListener {
    private List<String> keys = new ArrayList<>();

    @Override
    public void onEvent(CqEvent aCqEvent) {
      keys.add(aCqEvent.getKey().toString());
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    public List<String> getKeys() {
      return keys;
    }
  }

  public static class KeysCacheListener extends CacheListenerAdapter<Object, Object> {
    public List<String> keys = new CopyOnWriteArrayList<>();

    @Override
    public void afterCreate(EntryEvent event) {
      keys.add((String) event.getKey());
    }
  }

  public static EventsCqListner createAndExecuteCQ(QueryService queryService, String cqName,
      String query)
      throws CqExistsException, CqException, RegionNotFoundException {
    CqAttributesFactory cqaf = new CqAttributesFactory();
    EventsCqListner listenter = new EventsCqListner();
    cqaf.addCqListener(listenter);

    CqQuery cq = queryService.newCq(cqName, query, cqaf.create());
    cq.execute();
    return listenter;
  }
}
