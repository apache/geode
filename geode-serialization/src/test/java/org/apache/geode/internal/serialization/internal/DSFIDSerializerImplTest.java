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

package org.apache.geode.internal.serialization.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

public class DSFIDSerializerImplTest {

  @Test
  public void registerFails_forDuplicateFixedIds() {
    int byteSizeFixedId = 1;
    int shortSizeFixedId = 1024;
    DataSerializableFixedID mockFixedId = mock(DataSerializableFixedID.class);
    DSFIDSerializer serializer = new DSFIDSerializerImpl();

    serializer.register(byteSizeFixedId, mockFixedId.getClass());
    assertThatThrownBy(() -> serializer.register(byteSizeFixedId, mockFixedId.getClass()))
        .isInstanceOf(IllegalArgumentException.class);

    serializer.register(shortSizeFixedId, mockFixedId.getClass());
    assertThatThrownBy(() -> serializer.register(shortSizeFixedId, mockFixedId.getClass()))
        .isInstanceOf(IllegalArgumentException.class);
  }

}
