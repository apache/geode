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
package org.apache.geode.internal.util;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.geode.internal.GeodeVersion;
import org.apache.geode.internal.version.ComponentVersion;
import org.apache.geode.internal.version.DistributionVersion;

public class ProductVersionUtil {
  public static DistributionVersion getProductVersion() {
    final ServiceLoader<DistributionVersion> loader = ServiceLoader.load(DistributionVersion.class);
    final Iterator<DistributionVersion> loaderIter = loader.iterator();
    if (loaderIter.hasNext()) {
      return loaderIter.next();
    }
    return new GeodeVersion();
  }

  public static String getFullVersion() {
    ServiceLoader<ComponentVersion> loader = ServiceLoader.load(ComponentVersion.class);
    StringBuilder versionString = new StringBuilder();
    loader.forEach(v -> versionString
        .append("----------------------------------------\n")
        .append(v.getName()).append("\n")
        .append("----------------------------------------\n")
        .append(v.getDetails()));
    return versionString.toString();
  }
}
