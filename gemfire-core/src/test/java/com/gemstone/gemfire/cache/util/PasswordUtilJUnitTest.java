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
package com.gemstone.gemfire.cache.util;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.Assert;
import junit.framework.TestCase;

@Category(UnitTest.class)
public class PasswordUtilJUnitTest extends TestCase {
  public void testPasswordUtil() {
    String x = "password";
    String z = null;

    //System.out.println(x);
    String y = PasswordUtil.encrypt(x);
    //System.out.println(y);
    y = "encrypted(" + y + ")";
    z = PasswordUtil.decrypt(y);
    //System.out.println(z);
    assertEquals(x, z);
  }
}
