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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.test.fake.Fakes;

public class GetAllOpJUnitTest {

  private final ExecutablePool pool = mock(PoolImpl.class);
  private final GemFireCacheImpl cache = Fakes.cache();
  private final LocalRegion region = mock(LocalRegion.class);

  private ArrayList<Integer> keys;

  @Before
  public void setup() {
    when(region.getCache()).thenReturn(cache);
    ClientMetadataService cms = mock(ClientMetadataService.class);
    when(cache.getClientMetadataService()).thenReturn(cms);

    keys = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      keys.add(i);
    }
    Map<ServerLocation, HashSet> serverToFilterMap = new HashMap<>();
    when(cms.getServerToFilterMap(keys, region, true)).thenReturn(serverToFilterMap);
    ServerLocation serverLocation = new ServerLocation("localhost", 12345);
    serverToFilterMap.put(serverLocation, new HashSet(keys));
  }

  @Test
  public void singleHopGetAllShouldRetrySOECausedBySerialzationExp() {
    when(region.getFullPath()).thenReturn("/testRegion")
        .thenThrow(new ServerOperationException(new SerializationException("testRetry")))
        .thenReturn("/testRegion");
    VersionedObjectList vol = new VersionedObjectList();
    when(pool.execute(any())).thenReturn(vol);
    VersionedObjectList result = GetAllOp.execute(pool, region, keys, -1, null);
    assertThat(result.getKeys()).isEqualTo(keys);
    Mockito.verify(pool, times(1)).execute(any());
  }

  @Test(expected = ServerOperationException.class)
  public void singleHopGetAllShouldNotRetrySOENotCausedBySerialzationExp() {
    when(region.getFullPath()).thenReturn("/testRegion")
        .thenThrow(new ServerOperationException(new IOException("testRetry")))
        .thenReturn("/testRegion");
    VersionedObjectList vol = new VersionedObjectList();
    when(pool.execute(any())).thenReturn(vol);
    VersionedObjectList result = GetAllOp.execute(pool, region, keys, -1, null);
    assertThat(result).isNull();
    Mockito.verify(pool, times(0)).execute(any());
  }

  @Test(expected = BucketMovedException.class)
  public void singleHopGetAllShouldNotRetryForExceptionOtherThanSOE() {
    when(region.getFullPath()).thenReturn("/testRegion")
        .thenThrow(new BucketMovedException("testRetry"))
        .thenReturn("/testRegion");
    VersionedObjectList vol = new VersionedObjectList();
    when(pool.execute(any())).thenReturn(vol);
    VersionedObjectList result = GetAllOp.execute(pool, region, keys, -1, null);
    assertThat(result).isNull();
    Mockito.verify(pool, times(0)).execute(any());
  }

}
