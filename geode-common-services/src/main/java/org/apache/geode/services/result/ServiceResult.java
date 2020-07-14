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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The {@link ServiceResult} type is an attempt at the function construct of
 * <a href="https://www.vavr.io/vavr-docs/#_either">Either</a>. In this implementation a
 * {@link ServiceResult}
 * can define either success or failure (error) using the same type.
 *
 * @param <SuccessType> the return type for a successful operation.
 *
 * @since 1.14.0
 */
public interface ServiceResult<SuccessType> extends Result<SuccessType, String> {
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

  /**
   * Executes the provided {@link Consumer} if this {@link ServiceResult} is a success.
   *
   * @param consumer the {@link Consumer} to be executed on success.
   * @return this {@link ServiceResult}
   */
  default ServiceResult<SuccessType> ifSuccessful(
      Consumer<? super SuccessType> consumer) {
    return this;
  }

  /**
   * If the result of the operation has failed, invoke the specified consumer with the value,
   * otherwise do nothing.
   *
   * @param consumer block to be executed if a value is present
   * @throws NullPointerException if value is present and {@code consumer} is
   *         null
   */
  default ServiceResult<SuccessType> ifFailure(
      Consumer<? super String> consumer) {
    return this;
  }

}
