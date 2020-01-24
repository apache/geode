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
package org.apache.geode.cache.client.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

public class PutOpJUnitTest {
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private final VersionTag versionTag = mock(VersionTag.class);
  private final RegionEntry entry = mock(RegionEntry.class);
  private final VersionStamp versionStamp = mock(VersionStamp.class);
  private final Connection connection = mock(Connection.class);

  @Before
  public void setup() {
    when(event.getEventId()).thenReturn(new EventID());
    when(entry.getVersionStamp()).thenReturn(versionStamp);
  }

  @Test
  public void regularDeltaPutShouldNotRetryFlagInMessage() {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        event, Operation.UPDATE, false, false, null, false, false);
    assertFalse(putOp.getMessage().isRetry());
  }

  @Test
  public void regularPutShouldNotRetryFlagInMessage() {

    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", null,
        event, Operation.UPDATE, false, false, null, false, false);
    assertFalse(putOp.getMessage().isRetry());
  }

  @Test
  public void failedDeltaPutShouldSetRetryFlagInMessage() {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        event, Operation.UPDATE, false, false, null, true, false);
    assertTrue(putOp.getMessage().isRetry());
  }

  @Test
  public void putOpSetVersionTagIfRegionEntryInEntryEventIsNull() throws Exception {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        event, Operation.UPDATE,
        false, false, null, false, false);
    when(event.getRegionEntry()).thenReturn(null);

    putOp.checkForDeltaConflictAndSetVersionTag(versionTag, null);

    verify(event).setVersionTag(versionTag);
  }

  @Test
  public void putOpSetVersionTagIfNotADeltaUpdate() throws Exception {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        event, Operation.UPDATE,
        false, false, null, true, false);
    when(event.getRegionEntry()).thenReturn(entry);

    putOp.checkForDeltaConflictAndSetVersionTag(versionTag, null);

    verify(event).setVersionTag(versionTag);
  }

  @Test
  public void putOpSetVersionTagIfDeltaUpdateVersionInOrder() throws Exception {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        event, Operation.UPDATE,
        false, false, null, false, false);
    when(event.getRegionEntry()).thenReturn(entry);
    when(versionTag.getEntryVersion()).thenReturn(2);
    when(versionStamp.getEntryVersion()).thenReturn(1);

    putOp.checkForDeltaConflictAndSetVersionTag(versionTag, null);

    verify(event).setVersionTag(versionTag);
  }

  @Test
  public void putOpGetFullValueIfDeltaUpdateVersionOutOfOrder() throws Exception {
    Object object = new Object();
    PutOp.PutOpImpl putOp =
        spy(new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
            event, Operation.UPDATE,
            false, false, null, false, false));
    when(event.getRegionEntry()).thenReturn(entry);
    when(versionTag.getEntryVersion()).thenReturn(3);
    when(versionStamp.getEntryVersion()).thenReturn(1);
    doReturn(object).when(putOp).getFullValue(connection);

    putOp.checkForDeltaConflictAndSetVersionTag(versionTag, connection);

    verify(event).setNewValue(object);
  }
}
