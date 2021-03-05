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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
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
    when(mockRegion.getFilterProfile()).thenReturn(fprofile);
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
    assertFalse("still has this interest: " + fprofile.getKeysOfInterestFor(clientId),
        fprofile.hasKeysOfInterestFor(clientId, inv));
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
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(),
        InterestType.KEY);
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
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(),
        InterestType.REGULAR_EXPRESSION);
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
    fprofile.registerClientInterest(clientId,
        "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2",
          "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS,
          inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId,
        "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS);
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
    fprofile.registerClientInterest(clientId,
        "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2",
          "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS,
          inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertTrue(isClientInterested);
    fprofile.unregisterClientInterest(clientId, UnregisterAllInterest.singleton(),
        InterestType.FILTER_CLASS);
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
    fprofile.registerClientInterest(clientId,
        "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS, inv);
    if (twoClients) {
      fprofile.registerClientInterest("client2",
          "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS,
          inv);
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
      fprofile.registerClientInterest("client2",
          "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS,
          inv);
    }
    boolean isClientInterested = fprofile.hasFilterInterestFor(clientId, inv);
    assertFalse(isClientInterested);
    fprofile.unregisterClientInterest(clientId,
        "org.apache.geode.internal.cache.tier.sockets.TestFilter", InterestType.FILTER_CLASS);
    assertFalse(fprofile.hasFilterInterestFor(clientId, inv));
  }

  @Test
  public void testRegisterUnregisterClientInterestListAndVerifyKeysRegistered() {
    registerUnregisterClientInterestListAndVerifyKeysRegistered(false);
  }

  @Test
  public void testRegisterUnregisterClientInterestListInvAndVerifyKeysRegistered() {
    registerUnregisterClientInterestListAndVerifyKeysRegistered(true);
  }


  private void registerUnregisterClientInterestListAndVerifyKeysRegistered(
      boolean updatesAsInvalidates) {
    String clientId = "client";
    List<String> keys = Arrays.asList("K1", "K2");

    Set registeredKeys = fprofile.registerClientInterestList(clientId, keys, updatesAsInvalidates);
    int numKeys = updatesAsInvalidates ? fprofile.getKeysOfInterestInv(clientId).size()
        : fprofile.getKeysOfInterest(clientId).size();
    assertEquals(2, numKeys);
    assertTrue("Expected key not found in registered list.", registeredKeys.containsAll(keys));

    // Re-register same keys. The return should be empty.
    registeredKeys = fprofile.registerClientInterestList(clientId, keys, updatesAsInvalidates);
    numKeys = updatesAsInvalidates ? fprofile.getKeysOfInterestInv(clientId).size()
        : fprofile.getKeysOfInterest(clientId).size();
    assertEquals(2, numKeys);
    assertEquals(0, registeredKeys.size());

    // Register one old key and new. It should return only the new key.
    keys = Arrays.asList("K2", "K3");
    registeredKeys = fprofile.registerClientInterestList(clientId, keys, updatesAsInvalidates);
    numKeys = updatesAsInvalidates ? fprofile.getKeysOfInterestInv(clientId).size()
        : fprofile.getKeysOfInterest(clientId).size();
    assertEquals(3, numKeys);
    assertEquals(1, registeredKeys.size());
    assertTrue("Expected key not found in registered list.", registeredKeys.contains("K3"));

    // Keys Registered are K1, K2, K3
    keys = Arrays.asList("K1", "K2");
    registeredKeys = fprofile.unregisterClientInterestList(clientId, keys);
    numKeys = updatesAsInvalidates ? fprofile.getKeysOfInterestInv(clientId).size()
        : fprofile.getKeysOfInterest(clientId).size();
    assertEquals(1, numKeys);
    assertTrue("Expected keys not found in unregistered list.", keys.containsAll(registeredKeys));

    // Unregister previously unregistered key and a new key.
    keys = Arrays.asList("K2", "K3");
    registeredKeys = fprofile.unregisterClientInterestList(clientId, keys);
    // Once all the interest for client is removed, the client id is removed from the interest list
    // map.
    Set keySet = updatesAsInvalidates ? fprofile.getKeysOfInterestInv(clientId)
        : fprofile.getKeysOfInterest(clientId);
    assertNull(keySet);
    assertEquals(1, registeredKeys.size());
    assertTrue("Expected key not found in unregistered list.", registeredKeys.contains("K3"));

    // Unregister again, this should return empty set.
    registeredKeys = fprofile.unregisterClientInterestList(clientId, keys);
    assertTrue(registeredKeys.isEmpty());
  }

  @Test
  public void testRegisterUnregisterClientInterestListsAndVerifyKeysRegistered() {
    String clientId = "client";
    List<String> keys = Arrays.asList("K1", "K2");

    Set registeredKeys = fprofile.registerClientInterestList(clientId, keys, false);
    // Register interest with invalidates.
    keys = Arrays.asList("K3", "K4");
    registeredKeys = fprofile.registerClientInterestList(clientId, keys, true);

    // Unregister keys from both list.
    keys = Arrays.asList("K2", "K3", "K5"); // K5 is not registered.
    registeredKeys = fprofile.unregisterClientInterestList(clientId, keys);
    assertEquals(2, registeredKeys.size());
    assertFalse("Expected key not found in registered list.", registeredKeys.contains("K5"));

    Set keySet = fprofile.getKeysOfInterest(clientId);
    assertEquals(1, keySet.size());
    assertTrue("Expected key not found in registered list.", keySet.contains("K1"));

    keySet = fprofile.getKeysOfInterestInv(clientId);
    assertEquals(1, keySet.size());
    assertTrue("Expected key not found in registered list.", keySet.contains("K4"));
  }
}
