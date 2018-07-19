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
package org.apache.geode.test.dunit.examples;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.test.dunit.DistributedTestCase;


@SuppressWarnings("serial")
public class FixtureOrderingExampleTest extends DistributedTestCase {

  @Override
  public void preSetUp() throws Exception {
    System.out.println("@Override preSetUp");
  }

  @Before
  public void setUp() throws Exception {
    System.out.println("@Before setUp");
  }

  @Override
  public void postSetUp() throws Exception {
    System.out.println("@Override postSetUp");
  }

  @Override
  public void preTearDown() throws Exception {
    System.out.println("@Override preTearDown");
  }

  @After
  public void tearDown() throws Exception {
    System.out.println("@After tearDown");
  }

  @Override
  public void postTearDown() throws Exception {
    System.out.println("@Override postTearDown");
  }

  @Test
  public void demoOrdering() throws Exception {
    // nothing
  }
}
