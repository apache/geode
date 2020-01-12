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
package org.apache.geode.internal.protocol.protobuf.v1;

import java.util.function.Function;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ExceptionWithErrorCode;

@Experimental
public class Failure<SuccessType> implements Result<SuccessType> {
  private final ClientProtocol.ErrorResponse failureType;

  private Failure(ClientProtocol.ErrorResponse failureType) {
    this.failureType = failureType;
  }

  public static <T> Failure<T> of(BasicTypes.ErrorCode errorCode, String errorMessage) {
    return new Failure<>(ClientProtocol.ErrorResponse.newBuilder()
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(errorMessage))
        .build());
  }

  public static <T> Failure<T> of(Throwable exception) {
    BasicTypes.ErrorCode errorCode = getErrorCode(exception);

    return of(errorCode, getErrorMessage(exception));
  }

  private static BasicTypes.ErrorCode getErrorCode(Throwable exception) {
    BasicTypes.ErrorCode errorCode = BasicTypes.ErrorCode.SERVER_ERROR;
    if (exception instanceof ExceptionWithErrorCode) {
      errorCode = ((ExceptionWithErrorCode) exception).getErrorCode();
    }
    return errorCode;
  }

  private static String getErrorMessage(Throwable exception) {

    StringBuilder message = new StringBuilder(exception.toString());
    while (exception.getCause() != null) {
      message.append(" Caused by: " + exception.getCause());
      exception = exception.getCause();
    }

    return message.toString();
  }

  @Override
  public <T> T map(Function<SuccessType, T> successFunction,
      Function<ClientProtocol.ErrorResponse, T> errorFunction) {
    return errorFunction.apply(failureType);
  }

  @Override
  public SuccessType getMessage() {
    throw new RuntimeException("This is not a Success result");
  }

  @Override
  public ClientProtocol.ErrorResponse getErrorMessage() {
    return failureType;
  }
}
