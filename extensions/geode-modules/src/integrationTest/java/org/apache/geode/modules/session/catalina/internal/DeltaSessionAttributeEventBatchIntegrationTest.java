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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class DeltaSessionAttributeEventBatchIntegrationTest
    extends AbstractDeltaSessionIntegrationTest {

  @Test
  public void toDataAndFromDataShouldWorkProperly()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    List<DeltaSessionAttributeEvent> queue = new ArrayList<>();
    queue.add(new DeltaSessionDestroyAttributeEvent(SECOND_ATTRIBUTE_KEY));
    queue.add(new DeltaSessionUpdateAttributeEvent(FIRST_ATTRIBUTE_KEY, FIRST_ATTRIBUTE_VALUE));
    DeltaSessionAttributeEventBatch originalBatch =
        new DeltaSessionAttributeEventBatch(REGION_NAME, TEST_SESSION_ID, queue);
    DeltaSessionAttributeEventBatch deserializeBatch =
        (DeltaSessionAttributeEventBatch) serializeDeserializeObject(originalBatch);

    assertThat(deserializeBatch.getKey()).isEqualTo(originalBatch.getKey());
    assertThat(originalBatch.getEventQueue()).isEqualTo(originalBatch.getEventQueue());
    assertThat(deserializeBatch.getRegionName()).isEqualTo(originalBatch.getRegionName());
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void applyBatchShouldNotDoAnythingIfSessionIdDoesNotExist(RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    DeltaSessionAttributeEventBatch deltaSessionAttributeEventBatch =
        new DeltaSessionAttributeEventBatch(REGION_NAME, "fakeSessionId",
            Collections.singletonList(new DeltaSessionDestroyAttributeEvent(FIRST_ATTRIBUTE_KEY)));

    // Apply batch and verify local session entry is not modified.
    deltaSessionAttributeEventBatch.apply(server.getCache());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getAttribute(FIRST_ATTRIBUTE_KEY))
        .isEqualTo(FIRST_ATTRIBUTE_VALUE);
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getAttribute(SECOND_ATTRIBUTE_KEY))
        .isEqualTo(SECOND_ATTRIBUTE_VALUE);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void applyBatchShouldApplyRemoteEventsOnLocalSession(RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    List<DeltaSessionAttributeEvent> queue = new ArrayList<>();
    queue.add(new DeltaSessionDestroyAttributeEvent(SECOND_ATTRIBUTE_KEY));
    queue.add(new DeltaSessionUpdateAttributeEvent(FIRST_ATTRIBUTE_KEY, SECOND_ATTRIBUTE_VALUE));
    DeltaSessionAttributeEventBatch deltaSessionAttributeEventBatch =
        new DeltaSessionAttributeEventBatch(REGION_NAME, TEST_SESSION_ID, queue);

    // Apply batch and verify local session entry is modified.
    deltaSessionAttributeEventBatch.apply(server.getCache());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getAttribute(SECOND_ATTRIBUTE_KEY))
        .isNull();
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getAttribute(FIRST_ATTRIBUTE_KEY))
        .isEqualTo(SECOND_ATTRIBUTE_VALUE);
  }
}
