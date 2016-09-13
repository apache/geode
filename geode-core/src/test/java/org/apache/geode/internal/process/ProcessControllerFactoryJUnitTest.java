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
package org.apache.geode.internal.process;

import static org.junit.Assert.*;

import java.io.File;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class ProcessControllerFactoryJUnitTest {

  @After
  public void tearDown() throws Exception {
    enableAttachApi();
  }
  
  @Test
  public void testIsAttachAPIFound() throws Exception {
    validateProcessControllerFactory(true);
    disableAttachApi();
    validateProcessControllerFactory(false);
    enableAttachApi();
    validateProcessControllerFactory(true);
  }
  
  private void validateProcessControllerFactory(boolean isAttachAPIFound) throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertEquals(isAttachAPIFound, factory.isAttachAPIFound());
    if (isAttachAPIFound) {
      final ProcessControllerParameters parms = new NullMBeanControllerParameters();
      final ProcessController controller = factory.createProcessController(parms, ProcessUtils.identifyPid());
      assertTrue(controller instanceof MBeanProcessController);
    } else {
      final ProcessControllerParameters parms = new NullFileControllerParameters();
      final ProcessController controller = factory.createProcessController(parms, ProcessUtils.identifyPid());
      assertTrue(controller instanceof FileProcessController);
    }
  }
  
  private static void disableAttachApi() {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }

  private static void enableAttachApi() {
    System.clearProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API);
  }
  
  private static class NullMBeanControllerParameters implements ProcessControllerParameters {
    @Override
    public int getProcessId() {
      return 0;
    }
    @Override
    public ProcessType getProcessType() {
      return null;
    }
    @Override
    public ObjectName getNamePattern() {
      return null;
    }
    @Override
    public String getPidAttribute() {
      return null;
    }
    @Override
    public String getStatusMethod() {
      return null;
    }
    @Override
    public String getStopMethod() {
      return null;
    }
    @Override
    public String[] getAttributes() {
      return null;
    }
    @Override
    public Object[] getValues() {
      return null;
    }
    @Override
    public File getPidFile() {
      throw new UnsupportedOperationException("Not implemented by NullMBeanControllerParameters");
    }
    @Override
    public File getWorkingDirectory() {
      throw new UnsupportedOperationException("Not implemented by NullMBeanControllerParameters");
    }
  }

  private static class NullFileControllerParameters implements ProcessControllerParameters {
    @Override
    public int getProcessId() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public ProcessType getProcessType() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public ObjectName getNamePattern() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public String getPidAttribute() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public String getStatusMethod() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public String getStopMethod() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public String[] getAttributes() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public Object[] getValues() {
      throw new UnsupportedOperationException("Not implemented by NullFileControllerParameters");
    }
    @Override
    public File getPidFile() {
      return null;
    }
    @Override
    public File getWorkingDirectory() {
      return null;
    }
  }
}
