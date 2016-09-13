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
package org.apache.geode.internal.cache.partitioned.fixed;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/** 
* FixedPartitioningTestBase Tester. 
*/
@Category(UnitTest.class)
public class FixedPartitioningTestBaseJUnitTest { 


  @Test
  @SuppressWarnings("RedundantStringConstructorCall")
  public void testGenerateDate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

    Class[] classes = {int.class, String.class, String.class};

    Method method = FixedPartitioningTestBase.class.getDeclaredMethod("generateDate",classes);
    method.setAccessible(true);
    Date date = (Date)method.invoke(null, 5, "May", "MyDate1");
    Date date1 = (Date)method.invoke(null, 5, "May", new String("MyDate1"));
    assertEquals(date, date1);

    date = (Date)method.invoke(null, 5, "May", "Date");
    date1 = (Date)method.invoke(null, 5, "May", new String("Date"));
    assertEquals(date, date1);

    date = (Date)method.invoke(null, 5, "May", "MyDate2");
    date1 = (Date)method.invoke(null, 5, "May", new String("MyDate2"));
    assertEquals(date, date1);

    date = (Date)method.invoke(null, 5, "May", "MyDate3");
    date1 = (Date)method.invoke(null, 5, "May", new String("MyDate3"));
    assertEquals(date, date1);
  }
} 
