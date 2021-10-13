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

package org.apache.geode.cache.wan.internal.txgrouping;

import static java.lang.String.format;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.wan.internal.GatewaySenderTypeFactory;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MutableGatewaySenderAttributes;

public abstract class CommonTxGroupingGatewaySenderFactory {
  public static void validate(final @NotNull GatewaySenderTypeFactory factory,
      final @NotNull MutableGatewaySenderAttributes attributes) {
    if (attributes.isBatchConflationEnabled()) {
      throw new GatewaySenderException(
          format(
              "%s %s cannot be created with both group transaction events set to true and batch conflation enabled",
              factory.getType(), attributes.getId()));
    }
  }
}
