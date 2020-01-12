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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

import org.apache.geode.internal.util.BlobHelper;

public class EvictionAttributesImplTest {

  @Test
  public void dataSerializes() throws Exception {
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();
    byte[] bytes = BlobHelper.serializeToBlob(evictionAttributes);
    assertThat(bytes).isNotNull().isNotEmpty();
    assertThat(BlobHelper.deserializeBlob(bytes)).isNotSameAs(evictionAttributes)
        .isInstanceOf(EvictionAttributesImpl.class);
  }

  @Test
  public void serializes() throws Exception {
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();
    byte[] bytes = SerializationUtils.serialize(evictionAttributes);
    assertThat(bytes).isNotNull().isNotEmpty();
    assertThat((EvictionAttributesImpl) SerializationUtils.deserialize(bytes))
        .isNotSameAs(evictionAttributes)
        .isInstanceOf(EvictionAttributesImpl.class);
  }
}
