/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FilterProfileJUnitTest {

  private LocalRegion mockRegion;
  private FilterProfile fprofile;
  
  @Before
  public void setUp() {
    mockRegion = mock(LocalRegion.class);
    GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    when(mockCache.getCacheServers()).thenReturn(Collections.emptyList());
    when(mockRegion.getGemFireCache()).thenReturn(mockCache);
    fprofile = new FilterProfile(mockRegion);
  }

  @Test
  public void testUnregisterKey() {
    unregisterKey(false);
  }
  
  @Test
  public void testUnregisterKeyInv() {
    unregisterKey(true);
  }

  private void unregisterKey(boolean inv) {
    unregisterKey(inv, false);
    unregisterKey(inv, true);
  }
  
  private void unregisterKey(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object1234", InterestType.KEY, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object1234", InterestType.KEY, inv);
    }
    boolean isClientInterested = fprofile.hasKeysOfInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "Object1234", InterestType.KEY);
    assertFalse(fprofile.hasKeysOfInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterTwoKeys() {
    unregisterTwoKeys(false);
  }

  @Test
  public void testUnregisterTwoKeysInv() {
    unregisterTwoKeys(true);
  }

  private void unregisterTwoKeys(boolean inv) {
    unregisterTwoKeys(inv, false);
    unregisterTwoKeys(inv, true);
  }
  
  private void unregisterTwoKeys(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object1234", InterestType.KEY, inv);
    fprofile.registerClientInterest(clientId, "Object4567", InterestType.KEY, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object1234", InterestType.KEY, inv);
    }
    boolean isClientInterested = fprofile.hasKeysOfInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "Object1234", InterestType.KEY);
    fprofile.unregisterClientInterest(clientId, "Object4567", InterestType.KEY);
    assertFalse("still has this interest: " + fprofile.getKeysOfInterestFor(clientId), fprofile.hasKeysOfInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterAllKey() {
    unregisterAllKey(false);
  }

  @Test
  public void testUnregisterAllKeyInv() {
    unregisterAllKey(true);
  }

  private void unregisterAllKey(boolean inv) {
    unregisterAllKey(inv, false);
    unregisterAllKey(inv, true);
  }
  
  private void unregisterAllKey(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object1234", InterestType.KEY, inv);
    fprofile.registerClientInterest(clientId, "Object4567", InterestType.KEY, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object1234", InterestType.KEY, inv);
    }
    boolean isClientInterested = fprofile.hasKeysOfInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(), InterestType.KEY);
    assertFalse(fprofile.hasKeysOfInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterRegex() {
    unregisterRegex(false);
  }

  @Test
  public void testUnregisterRegexInv() {
    unregisterRegex(true);
  }

  private void unregisterRegex(boolean inv) {
    unregisterRegex(inv, false);
    unregisterRegex(inv, true);
  }
  
  private void unregisterRegex(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object.*", InterestType.REGULAR_EXPRESSION, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object.*", InterestType.REGULAR_EXPRESSION, inv);
    }
    boolean isClientInterested = fprofile.hasRegexInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "Object.*", InterestType.REGULAR_EXPRESSION);
    assertFalse(fprofile.hasRegexInterestFor(clientId, inv));
  }
  
  @Test
  public void testUnregisterAllRegex() {
    unregisterAllRegex(false);
  }

  @Test
  public void testUnregisterAllRegexInv() {
    unregisterAllRegex(true);
  }

  private void unregisterAllRegex(boolean inv) {
    unregisterAllRegex(inv, false);
    unregisterAllRegex(inv, true);
  }
  
  private void unregisterAllRegex(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, ".*", InterestType.REGULAR_EXPRESSION, inv);
    fprofile.registerClientInterest(clientId, "Object.*", InterestType.REGULAR_EXPRESSION, inv);
    fprofile.registerClientInterest(clientId, "Key.*", InterestType.REGULAR_EXPRESSION, inv);
    if (twoClients) {
      String clientId2 = "client2";
      fprofile.registerClientInterest(clientId2, ".*", InterestType.REGULAR_EXPRESSION, inv);
      fprofile.registerClientInterest(clientId2, "Object.*", InterestType.REGULAR_EXPRESSION, inv);
      fprofile.registerClientInterest(clientId2, "Key.*", InterestType.REGULAR_EXPRESSION, inv);
    }
    boolean isClientInterested = fprofile.hasRegexInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(), InterestType.REGULAR_EXPRESSION);
    assertFalse(fprofile.hasRegexInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterAllKeys() {
    unregisterAllKeys(false);
  }

  @Test
  public void testUnregisterAllKeysInv() {
    unregisterAllKeys(true);
  }

  private void unregisterAllKeys(boolean inv) {
    unregisterAllKeys(inv, false);
    unregisterAllKeys(inv, true);
  }
  
  private void unregisterAllKeys(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, ".*", InterestType.REGULAR_EXPRESSION, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", ".*", InterestType.REGULAR_EXPRESSION, inv);
    }
    boolean isClientInterested = fprofile.hasAllKeysInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, ".*", InterestType.REGULAR_EXPRESSION);
    assertFalse(fprofile.hasAllKeysInterestFor(clientId, inv));
  }
  
  @Test
  public void testUnregisterFilterClass() {
    unregisterFilterClass(false);
  }
  
  @Test
  public void testUnregisterFilterClassInv() {
    unregisterFilterClass(true);
  }
  
  private void unregisterFilterClass(boolean inv) {
    unregisterFilterClass(inv, false);
    unregisterFilterClass(inv, true);
  }

  private void unregisterFilterClass(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS);
    assertFalse(fprofile.hasFilterInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterAllFilterClass() {
    unregisterAllFilterClass(false);
  }
  
  @Test
  public void testUnregisterAllFilterClassInv() {
    unregisterAllFilterClass(true);
  }
  
  private void unregisterAllFilterClass(boolean inv) {
    unregisterAllFilterClass(inv, false);
    unregisterAllFilterClass(inv, true);
  }

  private void unregisterAllFilterClass(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(), InterestType.FILTER_CLASS);
    assertFalse(fprofile.hasFilterInterestFor(clientId, inv));
    
  }
  
  @Test
  public void testUnregisterRegexNotRegistered() {
    unregisterRegexNotRegistered(false);
  }

  @Test
  public void testUnregisterRegexNotRegisteredInv() {
    unregisterRegexNotRegistered(true);
  }
  
  private void unregisterRegexNotRegistered(boolean inv) {
    unregisterRegexNotRegistered(inv, false);
    unregisterRegexNotRegistered(inv, true);
  }
  
  private void unregisterRegexNotRegistered(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object.*", InterestType.REGULAR_EXPRESSION, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object.*", InterestType.REGULAR_EXPRESSION, inv);
    }
    boolean isClientInterested = fprofile.hasRegexInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "Key.*", InterestType.REGULAR_EXPRESSION);
    assertTrue(fprofile.hasRegexInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterKeyNotRegistered() {
    unregisterKeyNotRegistered(false);
  }

  @Test
  public void testUnregisterKeyNotRegisteredInv() {
    unregisterKeyNotRegistered(true);
  }
  
  private void unregisterKeyNotRegistered(boolean inv) {
    unregisterKeyNotRegistered(inv, false);
    unregisterKeyNotRegistered(inv, true);
  }
  
  private void unregisterKeyNotRegistered(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "Object1234", InterestType.KEY, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "Object1234", InterestType.KEY, inv);
    }
    boolean isClientInterested = fprofile.hasKeysOfInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "Object5678", InterestType.KEY);
    assertTrue(fprofile.hasKeysOfInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterFilterNotRegistered() {
    unregisterFilterNotRegistered(false);
  }

  @Test
  public void testUnregisterFilterNotRegisteredInv() {
    unregisterFilterNotRegistered(true);
  }
  
  private void unregisterFilterNotRegistered(boolean inv) {
    unregisterFilterNotRegistered(inv, false);
    unregisterFilterNotRegistered(inv, true);
  }
  
  private void unregisterFilterNotRegistered(boolean inv, boolean twoClients) {
    String clientId = "client";
    fprofile.registerClientInterest(clientId, "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2", "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "hmm", InterestType.FILTER_CLASS);
    assertTrue(fprofile.hasFilterInterestFor(clientId, inv));
  }

  @Test
  public void testUnregisterAllKeysNotRegistered() {
    unregisterAllKeysNotRegistered(false);
  }

  @Test
  public void testUnregisterAllKeysNotRegisteredInv() {
    unregisterAllKeysNotRegistered(true);
  }
  
  private void unregisterAllKeysNotRegistered(boolean inv) {
    unregisterAllKeysNotRegistered(inv, false);
    unregisterAllKeysNotRegistered(inv, true);
  }
  
  private void unregisterAllKeysNotRegistered(boolean inv, boolean twoClients) {
    String clientId = "client";
    if (twoClients) {
      fprofile.registerClientInterest("client2", ".*", InterestType.REGULAR_EXPRESSION, inv);
    }
    boolean isClientInterested = fprofile.hasAllKeysInterestFor(clientId, inv);
    assertFalse(isClientInterested);
    fprofile.unregisterClientInterest(clientId, ".*", InterestType.REGULAR_EXPRESSION);
    assertFalse(fprofile.hasAllKeysInterestFor(clientId, inv));
  }
  @Test
  public void testUnregisterAllFilterNotRegistered() {
    unregisterAllFilterNotRegistered(false);
  }

  @Test
  public void testUnregisterAllFilterNotRegisteredInv() {
    unregisterAllFilterNotRegistered(true);
  }
  
  private void unregisterAllFilterNotRegistered(boolean inv) {
    unregisterAllFilterNotRegistered(inv, false);
    unregisterAllFilterNotRegistered(inv, true);
  }
  
  private void unregisterAllFilterNotRegistered(boolean inv, boolean twoClients) {
    String clientId = "client";
    if (twoClients) {
      fprofile.registerClientInterest("client2", "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertFalse(isClientInterested);
    fprofile.unregisterClientInterest(clientId, "com.gemstone.gemfire.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS);
    assertFalse(fprofile.hasFilterInterestFor(clientId, inv));
  }
  
}
