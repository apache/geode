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
package org.apache.geode.internal.serialization.filter;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedClassNames;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class SanctionedSerializablesTest {

  @Test
  public void returnsAcceptListFromSanctionedSerializablesService() {
    Collection<SanctionedSerializablesService> services = new HashSet<>();
    services.add(serviceWithSanctionedSerializablesOf("foo", "bar"));

    Set<String> result = loadSanctionedClassNames(services);

    assertThat(result).containsExactlyInAnyOrder("foo", "bar");
  }

  @Test
  public void returnsAcceptListsFromManySanctionedSerializablesServices() {
    Collection<SanctionedSerializablesService> services = new HashSet<>();
    services.add(serviceWithSanctionedSerializablesOf("foo", "bar"));
    services.add(serviceWithSanctionedSerializablesOf("the", "fox"));
    services.add(serviceWithSanctionedSerializablesOf("a", "bear"));

    Set<String> result = loadSanctionedClassNames(services);

    assertThat(result).containsExactlyInAnyOrder("foo", "bar", "the", "fox", "a", "bear");
  }

  @Test
  public void nullThrowsNullPointerException() {
    Throwable thrown = catchThrowable(() -> {
      loadSanctionedClassNames(null);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void emptyServicesReturnsEmptySet() {
    Set<String> result = loadSanctionedClassNames(emptySet());

    assertThat(result).isEmpty();
  }

  @Test
  public void servicesWithEmptyAcceptListsReturnsEmptySet() {
    Collection<SanctionedSerializablesService> services = new HashSet<>();
    services.add(serviceWithSanctionedSerializablesOf());
    services.add(serviceWithSanctionedSerializablesOf());
    services.add(serviceWithSanctionedSerializablesOf());

    Set<String> result = loadSanctionedClassNames(services);

    assertThat(result).isEmpty();
  }

  private static SanctionedSerializablesService serviceWithSanctionedSerializablesOf(
      String... classNames) {
    try {
      SanctionedSerializablesService service = mock(SanctionedSerializablesService.class);
      when(service.getSerializationAcceptlist()).thenReturn(asList(classNames));
      return service;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
