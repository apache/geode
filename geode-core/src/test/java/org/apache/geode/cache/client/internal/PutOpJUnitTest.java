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

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;

public class PutOpJUnitTest {

  private EntryEventImpl getEntryEvent() {
    EntryEventImpl entryEvent = Mockito.mock(EntryEventImpl.class);
    Mockito.when(entryEvent.getEventId()).thenReturn(new EventID());
    return entryEvent;
  }

  @Test
  public void regularDeltaPutShouldNotRetryFlagInMessage() {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        getEntryEvent(), Operation.UPDATE,
        false, false, null, false, false);
    assertFalse(putOp.getMessage().isRetry());
  }

  @Test
  public void regularPutShouldNotRetryFlagInMessage() {

    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", null,
        getEntryEvent(), Operation.UPDATE,
        false, false, null, false, false);
    assertFalse(putOp.getMessage().isRetry());
  }

  @Test
  public void failedDeltaPutShouldSetRetryFlagInMessage() {
    PutOp.PutOpImpl putOp = new PutOp.PutOpImpl("testRegion", "testKey", "testValue", new byte[10],
        getEntryEvent(), Operation.UPDATE,
        false, false, null, true, false);
    assertTrue(putOp.getMessage().isRetry());
  }

}
