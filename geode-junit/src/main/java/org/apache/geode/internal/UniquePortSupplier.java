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
package org.apache.geode.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

/**
 * Supplies unique ports that have not already been supplied by this instance of PortSupplier
 */
public class UniquePortSupplier {

  private final IntSupplier supplier;
  private final Set<Integer> usedPorts = new HashSet<>();

  public UniquePortSupplier() {
    supplier = () -> AvailablePortHelper.getRandomAvailableTCPPort();
  }

  public UniquePortSupplier(IntSupplier supplier) {
    this.supplier = supplier;
  }

  public synchronized int getAvailablePort() {
    int result = IntStream.generate(supplier)
        .filter(port -> !usedPorts.contains(port))
        .findFirst()
        .getAsInt();

    usedPorts.add(result);
    return result;
  }
}
