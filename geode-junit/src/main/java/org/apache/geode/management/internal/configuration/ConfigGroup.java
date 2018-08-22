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
 *
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * thisright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * this of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.management.internal.configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigGroup implements Serializable {
  public String name;
  private Set<String> jars = new HashSet<>();
  private Set<String> configFiles = new HashSet<>();
  private Set<String> regions = new HashSet<>();
  private String maxLogFileSize;

  public ConfigGroup(ConfigGroup that) {
    this.jars.addAll(that.jars);
    this.configFiles.addAll(that.configFiles);
    this.regions.addAll(that.regions);
    this.maxLogFileSize = that.maxLogFileSize;
    this.name = that.name;
  }

  public ConfigGroup(String name) {
    this.name = name;
  }

  public ConfigGroup regions(String... regions) {
    this.regions.addAll(Arrays.asList(regions));
    return this;
  }

  public ConfigGroup jars(String... jars) {
    this.jars.addAll(Arrays.asList(jars));
    return this;
  }

  public ConfigGroup configFiles(String... configFiles) {
    this.configFiles.addAll(Arrays.asList(configFiles));
    return this;
  }

  public ConfigGroup removeJar(String jar) {
    this.jars.remove(jar);
    return this;
  }

  public ConfigGroup addJar(String jar) {
    this.jars.add(jar);
    return this;
  }

  public ConfigGroup maxLogFileSize(String maxLogFileSize) {
    this.maxLogFileSize = maxLogFileSize;
    return this;
  }

  public Set<String> getJars() {
    return Collections.unmodifiableSet(this.jars);
  }

  public Set<String> getAllFiles() {
    return Collections.unmodifiableSet(
        Stream.concat(this.jars.stream(), this.configFiles.stream()).collect(Collectors.toSet()));
  }

  public Set<String> getAllJarFiles() {
    return this.jars.stream().collect(Collectors.toSet());
  }

  public Set<String> getRegions() {
    return Collections.unmodifiableSet(this.regions);
  }

  public String getName() {
    return this.name;
  }

  public String getMaxLogFileSize() {
    return this.maxLogFileSize;
  }
}
