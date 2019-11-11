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

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.modules.session.catalina.DeltaSession;

@RunWith(JUnitParamsRunner.class)
public class DeltaSessionUpdateAttributeEventIntegrationTest
    extends AbstractDeltaSessionIntegrationTest {

  @Test
  public void toDataAndFromDataShouldWorkProperly()
      throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    DeltaSessionUpdateAttributeEvent originalEvent =
        new DeltaSessionUpdateAttributeEvent(FIRST_ATTRIBUTE_KEY, FIRST_ATTRIBUTE_VALUE);
    DeltaSessionUpdateAttributeEvent deserializeEvent =
        (DeltaSessionUpdateAttributeEvent) serializeDeserializeObject(originalEvent);

    assertThat(deserializeEvent.getAttributeName()).isEqualTo(originalEvent.getAttributeName());
    assertThat(deserializeEvent.getAttributeValue()).isEqualTo(originalEvent.getAttributeValue());
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void applyShouldUpdateTheSessionAttributeWithinTheLocalCacheEntry(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    DeltaSessionUpdateAttributeEvent updateAttributeEvent =
        new DeltaSessionUpdateAttributeEvent(FIRST_ATTRIBUTE_KEY, SECOND_ATTRIBUTE_VALUE);

    // Apply event and verify local session entry is modified.
    DeltaSession deltaSessionInterface = (DeltaSession) httpSessionRegion.get(TEST_SESSION_ID);
    updateAttributeEvent.apply(deltaSessionInterface);
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getAttribute(FIRST_ATTRIBUTE_KEY))
        .isEqualTo(SECOND_ATTRIBUTE_VALUE);
  }
}
