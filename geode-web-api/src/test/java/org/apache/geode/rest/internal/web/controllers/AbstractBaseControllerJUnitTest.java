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
package org.apache.geode.rest.internal.web.controllers;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * AbstractBaseController Tester.
 */
@Category(UnitTest.class)
public class AbstractBaseControllerJUnitTest {

  AbstractBaseController abstractBaseController = new AbstractBaseController() {
    @Override
    protected String getRestApiVersion() {
      return "1.0";
    }
  };
  ObjectMapper mapper = new ObjectMapper();

  /**
   * Method: convertErrorAsJson(String errorMessage)
   */
  @Test
  public void testConvertErrorAsJsonErrorCause() throws Exception {
    String message = "This is an error message";
    String json = abstractBaseController.convertErrorAsJson(message);
    ErrorCause errorCause = mapper.readValue(json, ErrorCause.class);
    assertEquals(message, errorCause.cause);
  }

  /**
   * Method: convertErrorAsJson(Throwable t)
   */
  @Test
  public void testConvertErrorAsJsonT() throws Exception {
    String message = "This is an error message";
    Exception e = new GemfireRestException(message, new NullPointerException());
    String json = abstractBaseController.convertErrorAsJson(e);
    ErrorMessage errorMessage = mapper.readValue(json, ErrorMessage.class);
    assertEquals(message, errorMessage.message);
  }

  public static class ErrorMessage {
    private String message;

    public String getMessage() {
      return message;
    }

    public void setMessage(final String message) {
      this.message = message;
    }
  }

  public static class ErrorCause {
    private String cause;

    public String getCause() {
      return cause;
    }

    public void setCause(final String cause) {
      this.cause = cause;
    }
  }

}
