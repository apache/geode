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

import java.io.ObjectInputStream;

/**
 * Defines operation to set this serialization filter on an {@code ObjectInputStream}.
 */
@FunctionalInterface
public interface StreamSerialFilter {

  /**
   * Sets this serialization filter on the specified {@code ObjectInputStream}.
   *
   * @throws FilterAlreadyConfiguredException if a non-null serialization filter already exists
   * @throws UnableToSetSerialFilterException if there's any failure setting a serialization filter
   */
  void setFilterOn(ObjectInputStream objectInputStream) throws UnableToSetSerialFilterException;
}
