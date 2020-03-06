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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsCacheWriterTest {

  private final ExportLogsCacheWriter writer = new ExportLogsCacheWriter();

  @Test
  public void writerReturnNullIfNoWrite() throws Exception {
    writer.startFile("server-1");
    assertThat(writer.endFile()).isNull();
  }

  @Test
  public void writerReturnsPathIfWritten() throws Exception {
    writer.startFile("server-1");
    @SuppressWarnings("unchecked")
    EntryEvent<String, byte[]> event = mock(EntryEvent.class);
    when(event.getNewValue()).thenReturn(new byte[] {});
    writer.beforeCreate(event);
    assertThat(writer.endFile()).isNotNull();
  }
}
