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
package org.apache.geode.management.internal.configuration.validators;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.internal.CacheElementOperation;

public interface ConfigurationValidator<T extends AbstractConfiguration> {

  /**
   * This is used to validate the configuration object passed in by the user
   *
   * This will be called after the ClusterManagementService received the configuration object from
   * the api call and before passing it to the realizers and mutators.
   *
   * @param operation the operation being performed. Different validation may be required depending
   *        on the operation.
   * @param config the user defined configuration object. It is mutable. you can modify the
   *        values in the configuration object. e.g. add default values
   *
   */
  void validate(CacheElementOperation operation, T config) throws IllegalArgumentException;
}
