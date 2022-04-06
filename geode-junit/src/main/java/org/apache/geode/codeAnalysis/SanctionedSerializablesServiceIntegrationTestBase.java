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
package org.apache.geode.codeAnalysis;

import static org.apache.geode.internal.serialization.SanctionedSerializables.loadSanctionedSerializablesServices;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

import org.junit.Test;

import org.apache.geode.internal.serialization.SanctionedSerializablesService;

public abstract class SanctionedSerializablesServiceIntegrationTestBase {

  protected abstract SanctionedSerializablesService getService();

  protected abstract ServiceResourceExpectation getServiceResourceExpectation();

  protected enum ServiceResourceExpectation {
    NULL,
    EMPTY,
    NON_EMPTY
  }

  @Test
  public final void serviceIsLoaded() {
    Collection<SanctionedSerializablesService> services = loadSanctionedSerializablesServices();
    SanctionedSerializablesService service = getService();

    boolean found = false;
    for (SanctionedSerializablesService aService : services) {
      if (service.getClass().equals(aService.getClass())) {
        found = true;
        break;
      }
    }

    assertThat(found)
        .as("Services " + services + " contains " + getService().getClass().getSimpleName())
        .isTrue();
  }

  @Test
  public final void serviceResourceExists() {
    SanctionedSerializablesService service = getService();

    URL url = service.getSanctionedSerializablesURL();

    switch (getServiceResourceExpectation()) {
      case NULL:
        assertThat(url).isNull();
        break;
      case EMPTY:
      case NON_EMPTY:
        assertThat(url).isNotNull();
    }
  }

  @Test
  public final void serviceResourceIsLoaded() throws IOException {
    SanctionedSerializablesService service = getService();

    Collection<String> serializables = service.getSerializationAcceptlist();

    switch (getServiceResourceExpectation()) {
      case NULL:
      case EMPTY:
        assertThat(serializables).isEmpty();
        break;
      case NON_EMPTY:
        assertThat(serializables).isNotEmpty();
    }
  }
}
