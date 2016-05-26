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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Subclass of ServerLauncherLocalDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar.  As a result ServerLauncher
 * ends up using the FileProcessController implementation.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class ServerLauncherLocalFileIntegrationTest extends ServerLauncherLocalIntegrationTest {

  @Before
  public final void setUpServerLauncherLocalFileTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }
  
  @After
  public final void tearDownServerLauncherLocalFileTest() throws Exception {   
  }
  
  @Override
  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());
  }
}
