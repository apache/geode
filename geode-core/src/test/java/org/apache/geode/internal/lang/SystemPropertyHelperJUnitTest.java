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
package org.apache.geode.internal.lang;

import static org.junit.Assert.*;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class SystemPropertyHelperJUnitTest {
  String preventSetOpBootstrapTransaction = "preventSetOpBootstrapTransaction";

  @Test
  public void testPreventSetOpBootstrapTransactionDefaultToFalse() {
    assertFalse(SystemPropertyHelper.preventSetOpBootstrapTransaction());
  }

  @Test
  public void testPreventSetOpBootstrapTransactionSystemProperty() {
    String gemfirePrefixProperty = "gemfire." + preventSetOpBootstrapTransaction;
    System.setProperty(gemfirePrefixProperty, "true");
    assertTrue(SystemPropertyHelper.preventSetOpBootstrapTransaction());
    System.clearProperty(gemfirePrefixProperty);

    String geodePrefixProperty = "geode." + preventSetOpBootstrapTransaction;
    System.setProperty(geodePrefixProperty, "true");
    assertTrue(SystemPropertyHelper.preventSetOpBootstrapTransaction());
    System.clearProperty(geodePrefixProperty);
  }

  @Test
  public void testPreventSetOpBootstrapTransactionGeodePreference() {
    String gemfirePrefixProperty = "gemfire." + preventSetOpBootstrapTransaction;
    String geodePrefixProperty = "geode." + preventSetOpBootstrapTransaction;
    System.setProperty(geodePrefixProperty, "false");
    System.setProperty(gemfirePrefixProperty, "true");
    assertFalse(SystemPropertyHelper.preventSetOpBootstrapTransaction());
    System.clearProperty(geodePrefixProperty);
    System.clearProperty(gemfirePrefixProperty);
  }

}
