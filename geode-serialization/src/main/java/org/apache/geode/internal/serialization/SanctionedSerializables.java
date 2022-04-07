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
package org.apache.geode.internal.serialization;

import static java.util.Collections.emptyList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ServiceLoader;

public class SanctionedSerializables {

  /**
   * Loads all SanctionedSerializablesServices on the classpath.
   */
  public static Collection<SanctionedSerializablesService> loadSanctionedSerializablesServices() {
    ServiceLoader<SanctionedSerializablesService> loader =
        ServiceLoader.load(SanctionedSerializablesService.class);
    Collection<SanctionedSerializablesService> services = new ArrayList<>();
    for (SanctionedSerializablesService service : loader) {
      services.add(service);
    }
    return services;
  }

  /**
   * Loads class names of sanctioned serializables from a resource. Caller will add these to the
   * serialization filter acceptlist.
   */
  public static Collection<String> loadClassNames(URL sanctionedSerializables) throws IOException {
    if (sanctionedSerializables == null) {
      return emptyList();
    }
    Collection<String> result = new ArrayList<>(1000);
    try (InputStream inputStream = sanctionedSerializables.openStream();
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = in.readLine()) != null) {
        line = line.trim();
        if (!(line.startsWith("#") || line.startsWith("//"))) {
          line = line.replaceAll("/", ".");
          result.add(line.substring(0, line.indexOf(',')));
        }
      }
    }
    return result;
  }
}
