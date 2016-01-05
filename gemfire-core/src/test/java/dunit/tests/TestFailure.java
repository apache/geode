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
package dunit.tests;

import dunit.*;

/**
 * The tests in this class always fail.  It is used when developing
 * DUnit to give us an idea of how test failure are logged, etc.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class TestFailure extends DistributedTestCase {

  public TestFailure(String name) {
    super(name);
  }

  ////////  Test Methods

  public void testFailure() {
    assertTrue("Test Failure", false);
  }

  public void testError() {
    String s = "Test Error";
    throw new Error(s);
  }

  public void testHang() throws InterruptedException {
    Thread.sleep(100000 * 1000);
  }

}
