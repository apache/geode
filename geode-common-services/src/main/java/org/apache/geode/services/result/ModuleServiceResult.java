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

package org.apache.geode.services.result;

import java.util.function.Function;

/**
 * The {@link ModuleServiceResult} type is an attempt at the function construct of
 * <a href="https://www.vavr.io/vavr-docs/#_either">Either</a>. In this implementation a
 * {@link ModuleServiceResult}
 * can define either success or failure (error) using the same type.
 *
 * @param <SuccessType> the return type for a successful operation.
 *
 * @since 1.14.0
 */
public interface ModuleServiceResult<SuccessType> extends Result<SuccessType, String> {
  /**
   * {@inheritDoc}
   */
  @Override
  <T> T map(Function<SuccessType, T> successFunction, Function<String, T> errorFunction);

  /**
   * {@inheritDoc}
   */
  @Override
  SuccessType getMessage();

  /**
   * {@inheritDoc}
   */
  @Override
  String getErrorMessage();
}
