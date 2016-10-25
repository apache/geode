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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/** 
* CacheServerLauncher Tester. 
*/
@Category(UnitTest.class)
public class CacheServerLauncherJUnitTest {

  @Test
  public void testSafeEquals(){
    String string1 = "string1";

    String string2 = string1;

    @SuppressWarnings("RedundantStringConstructorCall")
    String string3 = new String(string1);

    assertTrue(CacheServerLauncher.safeEquals(string1, string2));
    assertTrue(CacheServerLauncher.safeEquals(string1, string3));
    assertTrue(CacheServerLauncher.safeEquals(null, null));
    assertFalse(CacheServerLauncher.safeEquals(null, string3));
    assertFalse(CacheServerLauncher.safeEquals(string1, null));

    
  }


} 
