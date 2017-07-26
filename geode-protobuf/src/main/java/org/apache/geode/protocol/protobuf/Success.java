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
package org.apache.geode.protocol.protobuf;

import java.util.function.Function;

public class Success<SuccessType> implements Result<SuccessType> {
  private final SuccessType successResponse;

  public Success(SuccessType successResponse) {
    this.successResponse = successResponse;
  }

  public static <T> Success<T> of(T result) {
    return new Success<>(result);
  }

  @Override
  public <T> T map(Function<SuccessType, T> successFunction,
      Function<BasicTypes.ErrorResponse, T> errorFunction) {
    return successFunction.apply(successResponse);
  }

  @Override
  public SuccessType getMessage() {
    return successResponse;
  }

  @Override
  public BasicTypes.ErrorResponse getErrorMessage() {
    throw new RuntimeException("This is a not Failure result");
  }
}
