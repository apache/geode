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

import static java.util.Objects.requireNonNull;

/**
 * The Java package containing {@code ObjectInputFilter}. On Java 8, it's {@code sun.misc}. On
 * Java 9 or greater, it's {@code java.io}.
 */
enum ApiPackage {

  JAVA_IO("java.io."),
  SUN_MISC("sun.misc.");

  private final String prefix;

  ApiPackage(String prefix) {
    this.prefix = prefix;
  }

  String getPrefix() {
    return prefix;
  }

  String qualify(String className) {
    return prefix + requireNonNull(className, "className is required");
  }
}
