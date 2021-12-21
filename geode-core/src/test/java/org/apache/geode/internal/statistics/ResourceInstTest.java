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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceType;
import org.apache.geode.internal.statistics.StatArchiveReader.StatArchiveFile;

/**
 * Unit tests for {@link ResourceInst}.
 *
 * <p>
 * Confirms ResourceInst equals change to fix GEODE-1782.
 *
 * <p>
 * GEODE-1782: StatArchiveReader ignores later stats resource with same name as closed stats
 * resource
 *
 * @since Geode 1.0
 */
public class ResourceInstTest {

  @Mock
  private StatArchiveFile archive1;
  @Mock
  private StatArchiveFile archive2;
  @Mock
  private ResourceType resourceType;

  @Before
  public void setUp() throws Exception {
    initMocks(this);
  }

  @Test
  public void sameFirstTSidxEquals() throws Exception {
    ResourceInst resourceInst1 =
        new ResourceInst(archive1, 0, "name", 0, resourceType, false);
    setFirstTSidx(resourceInst1, 1);
    ResourceInst resourceInst2 =
        new ResourceInst(archive1, 0, "name", 0, resourceType, false);
    setFirstTSidx(resourceInst2, 1);

    assertThat(resourceInst1).isEqualTo(resourceInst2);
  }

  @Test
  public void differentFirstTSidxIsNotEqual() throws Exception {
    ResourceInst resourceInst1 =
        new ResourceInst(archive1, 0, "name", 0, resourceType, false);
    setFirstTSidx(resourceInst1, 1);
    ResourceInst resourceInst2 =
        new ResourceInst(archive1, 0, "name", 0, resourceType, false);
    setFirstTSidx(resourceInst2, 2);

    assertThat(resourceInst1).isNotEqualTo(resourceInst2);
  }

  private void setFirstTSidx(ResourceInst resourceInst, int value)
      throws IllegalAccessException, NoSuchFieldException {
    Field field = ResourceInst.class.getDeclaredField("firstTSidx");
    field.setAccessible(true);
    field.setInt(resourceInst, value);
  }

}
