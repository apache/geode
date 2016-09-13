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
package org.apache.geode.distributed;

import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertFalse;

/**
 * Subclass of LocatorLauncherLocalDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar. As a result LocatorLauncher
 * ends up using the FileProcessController implementation.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorLauncherLocalFileIntegrationTest extends LocatorLauncherLocalIntegrationTest {

  @Before
  public final void setUpLocatorLauncherLocalFileIntegrationTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }
  
  @After
  public final void tearDownLocatorLauncherLocalFileIntegrationTest() throws Exception {
  }
  
  @Override
  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());
  }
}
