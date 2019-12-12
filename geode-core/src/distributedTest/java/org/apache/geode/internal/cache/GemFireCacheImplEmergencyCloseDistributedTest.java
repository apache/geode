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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class GemFireCacheImplEmergencyCloseDistributedTest {

  private VM vm;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(1);

  @Before
  public void setUp() {
    vm = getVM(0);
  }

  @After
  public void tearDown() {
    // This is to close the resources that are left over after emergencyClose().
    vm.bounceForcibly();
  }

  @Test
  public void emergencyClose_closesCache() {
    vm.invoke(() -> {
      Cache cache = new CacheFactory().create();

      GemFireCacheImpl.emergencyClose();

      assertThat(cache.isClosed())
          .isTrue();
    });
  }
}
