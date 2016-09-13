/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.web.shell;

import static org.junit.Assert.*;

import java.util.Collections;

import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.management.internal.web.AbstractWebTestCase;
import org.apache.geode.management.internal.web.domain.Link;
import org.apache.geode.management.internal.web.http.ClientHttpRequest;
import org.apache.geode.management.internal.web.http.HttpHeader;
import org.apache.geode.management.internal.web.http.HttpMethod;
import org.apache.geode.test.junit.categories.UnitTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;

/**
 * The SimpleHttpOperationInvokerJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * SimpleHttpOperationInvoker class.
 * <p/>
 * @see org.apache.geode.management.internal.web.AbstractWebTestCase
 * @see org.apache.geode.management.internal.web.shell.SimpleHttpOperationInvoker
 * @see org.junit.Assert
 * @see org.junit.After
 * @see org.junit.Before
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class SimpleHttpOperationInvokerJUnitTest extends AbstractWebTestCase {

  private SimpleHttpOperationInvoker operationInvoker;

  @Before
  public void setUp() {
    operationInvoker = new SimpleHttpOperationInvoker();
  }

  @After
  public void tearDown() {
    operationInvoker.stop();
    operationInvoker = null;
  }

  private CommandRequest createCommandRequest(final String command) {
    return new TestCommandRequest(command);
  }

  private String getExpectedHttpRequestUrl(final CommandRequest command) {
    return SimpleHttpOperationInvoker.REST_API_URL.concat(SimpleHttpOperationInvoker.REST_API_MANAGEMENT_COMMANDS_URI)
      .concat("?").concat(SimpleHttpOperationInvoker.CMD_QUERY_PARAMETER).concat("=").concat(command.getInput());
  }

  private SimpleHttpOperationInvoker getOperationInvoker() {
    return operationInvoker;
  }

  @Test
  public void testCreateHttpRequest() throws Exception {
    final CommandRequest command = createCommandRequest("save resource --path=/path/to/file --size=1024KB");

    final ClientHttpRequest request = getOperationInvoker().createHttpRequest(command);

    assertNotNull(request);
    assertEquals(SimpleHttpOperationInvoker.USER_AGENT_HTTP_REQUEST_HEADER_VALUE,
      request.getHeaderValue(HttpHeader.USER_AGENT.getName()));

    final Link requestLink = request.getLink();

    assertNotNull(requestLink);
    assertTrue(toString(requestLink).startsWith("POST"));
    assertTrue(toString(requestLink).endsWith(command.getInput()));
  }

  @Test
  public void testCreateLink() throws Exception {
    final CommandRequest command = createCommandRequest("delete resource --id=1");

    final Link actualLink = getOperationInvoker().createLink(command);

    assertNotNull(actualLink);
    assertEquals(SimpleHttpOperationInvoker.LINK_RELATION, actualLink.getRelation());
    assertEquals(HttpMethod.POST, actualLink.getMethod());
    assertTrue(toString(actualLink.getHref()).endsWith(command.getInput()));
  }

  @Test
  public void testGetHttpRequestUrl() throws Exception {
    final CommandRequest command = createCommandRequest("get resource --option=value");

    assertEquals(getExpectedHttpRequestUrl(command), toString(getOperationInvoker().getHttpRequestUrl(command)));
  }

  @Test
  public void testProcessCommand() {
    final String expectedResult = "<resource>test</resource>"; // XML

    final SimpleHttpOperationInvoker operationInvoker = new SimpleHttpOperationInvoker() {
      @Override
      public boolean isConnected() {
        return true;
      }

      @Override
      @SuppressWarnings("unchecked")
      protected <T> ResponseEntity<T> send(final ClientHttpRequest request, final Class<T> responseType) {
        return new ResponseEntity(expectedResult, HttpStatus.OK);
      }
    };

    final String actualResult = operationInvoker.processCommand(createCommandRequest("get resource --id=1"));

    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testProcessCommandHandlesResourceAccessException() {
    final SimpleHttpOperationInvoker operationInvoker = new SimpleHttpOperationInvoker() {
      private boolean connected = true;
      @Override
      public boolean isConnected() {
        return connected;
      }

      @Override
      protected <T> ResponseEntity<T> send(final ClientHttpRequest request, final Class<T> responseType) {
        throw new ResourceAccessException("test");
      }

      @Override public void stop() {
        this.connected = false;
      }
    };

    assertTrue(operationInvoker.isConnected());

    final String expectedResult = String.format(
      "The connection to the GemFire Manager's HTTP service @ %1$s failed with: %2$s. "
        + "Please try reconnecting or see the GemFire Manager's log file for further details.",
          operationInvoker.getBaseUrl(), "test");

    final String actualResult = operationInvoker.processCommand(createCommandRequest("get resource --id=1"));

    assertFalse(operationInvoker.isConnected());
    assertEquals(expectedResult, actualResult);
  }

  @Test(expected = IllegalStateException.class)
  public void testProcessCommandWhenNotConnected() {
    try {
      getOperationInvoker().processCommand(createCommandRequest("get resource"));
    }
    catch (IllegalStateException e) {
      assertEquals("Gfsh must be connected to the GemFire Manager in order to process commands remotely!",
        e.getMessage());
      throw e;
    }
  }

  private static final class TestCommandRequest extends CommandRequest {

    private final String command;

    protected TestCommandRequest(final String command) {
      super(Collections.<String, String>emptyMap());
      assert command != null : "The command cannot be null!";
      this.command = command;
    }

    @Override
    public String getInput() {
      return command;
    }
  }

}
