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

package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

public class DeltaSessionAttributeEventBatchTest {
  String regionName = "regionName";
  String sessionId = "sessionId";
  LogWriter logWriter = mock(LogWriter.class);

  @Test
  public void TestApplyForBatch() {

    final List<DeltaSessionAttributeEvent> eventList = new ArrayList<>();
    final DeltaSessionAttributeEvent event1 = mock(DeltaSessionAttributeEvent.class);
    final DeltaSessionAttributeEvent event2 = mock(DeltaSessionAttributeEvent.class);
    eventList.add(event1);
    eventList.add(event2);

    final Cache cache = mock(Cache.class);
    final Region<String, DeltaSessionInterface> region = mock(Region.class);
    final DeltaSessionInterface deltaSessionInterface = mock(DeltaSessionInterface.class);

    doReturn(region).when(cache).getRegion(regionName);
    when(cache.getLogger()).thenReturn(logWriter);
    when(logWriter.fineEnabled()).thenReturn(false);
    when(region.get(sessionId)).thenReturn(deltaSessionInterface);

    final DeltaSessionAttributeEventBatch batch =
        new DeltaSessionAttributeEventBatch(regionName, sessionId, eventList);

    batch.apply(cache);

    verify(deltaSessionInterface).applyAttributeEvents(region, eventList);
  }
}
