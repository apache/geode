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

package org.apache.geode.management.internal.cli.commands;


import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CreateAlterDestroyRegionCommandsTest {

  @Rule
  public GfshParserRule parser = new GfshParserRule();

  @Test
  public void testCreateRegionWithInvalidPartitionResolver() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    DistributedSystemMXBean dsMBean = mock(DistributedSystemMXBean.class);
    CreateAlterDestroyRegionCommands spy =
        parser.spyCommand("create region --name=region3 --type=PARTITION --partition-resolver=Foo");
    doReturn(cache).when(spy).getCache();
    doReturn(dsMBean).when(spy).getDSMBean(cache);

    assertThatThrownBy(() -> parser.executeLastCommandWithInstance(spy))
        .hasMessageContaining("Foo is an invalid Partition Resolver");
  }
}
