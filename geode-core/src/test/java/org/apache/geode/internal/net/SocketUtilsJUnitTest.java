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
package org.apache.geode.internal.net;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The SocketUtilsJUnitTest class is a test suite of test cases testing the contract and functionality of the SocketUtils
 * utility class.
 * <p/>
 * @see org.apache.geode.internal.net.SocketUtils
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class SocketUtilsJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
  }

  @Test
  public void testCloseSocket() throws IOException {
    final Socket mockSocket = mockContext.mock(Socket.class, "closeSocketTest");

    mockContext.checking(new Expectations() {{
      oneOf(mockSocket).close();
    }});

    assertTrue(SocketUtils.close(mockSocket));
  }

  @Test
  public void testCloseSocketThrowsIOException() throws IOException {
    final Socket mockSocket = mockContext.mock(Socket.class, "closeSocketThrowsIOExceptionTest");

    mockContext.checking(new Expectations() {{
      oneOf(mockSocket).close();
      will(throwException(new IOException("test")));
    }});

    try {
      assertFalse(SocketUtils.close(mockSocket));
    }
    catch (Throwable t) {
      fail("Calling close on a Socket using SocketUtils threw an unexpected Throwable (" + t + ")!");
    }
  }

  @Test
  public void testCloseSocketWithNull() {
    assertTrue(SocketUtils.close((Socket) null));
  }

  @Test
  public void testCloseServerSocket() throws IOException {
    final ServerSocket mockServerSocket = mockContext.mock(ServerSocket.class, "closeServerSocketTest");

    mockContext.checking(new Expectations() {{
      oneOf(mockServerSocket).close();
    }});

    assertTrue(SocketUtils.close(mockServerSocket));
  }

  @Test
  public void testCloseServerSocketThrowsIOException() throws IOException {
    final ServerSocket mockServerSocket = mockContext.mock(ServerSocket.class, "closeServerSocketThrowsIOExceptionTest");

    mockContext.checking(new Expectations() {{
      oneOf(mockServerSocket).close();
      will(throwException(new IOException("test")));
    }});

    try {
      assertFalse(SocketUtils.close(mockServerSocket));
    }
    catch (Throwable t) {
      fail("Calling close on a ServerSocket using SocketUtils threw an unexpected Throwable (" + t + ")!");
    }
  }

  @Test
  public void testCloseServerSocketWithNull() {
    assertTrue(SocketUtils.close((ServerSocket) null));
  }

}
