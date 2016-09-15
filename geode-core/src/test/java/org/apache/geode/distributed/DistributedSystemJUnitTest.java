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

import static org.assertj.core.api.Assertions.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for DistributedSystem class.
 */
@Category(UnitTest.class)
public class DistributedSystemJUnitTest {
  
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Test
  public void getPropertiesFileShouldUseDefault() throws Exception {
    assertThat(DistributedSystem.getPropertiesFile()).isEqualTo(DistributedSystem.PROPERTIES_FILE_DEFAULT);
  }
  
  @Test
  public void getPropertiesFileShouldUseSystemProperty() throws Exception {
    String propertiesFileName = "test.properties";
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFileName);
    
    assertThat(DistributedSystem.getPropertiesFile()).isEqualTo(propertiesFileName);
  }
  
  @Test
  public void getPropertiesFileShouldUseSystemPropertyPath() throws Exception {
    String propertiesFileName = "/home/test.properties";
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFileName);
    
    assertThat(DistributedSystem.getPropertiesFile()).isEqualTo(propertiesFileName);
  }
  
  @Test
  public void getSecurityPropertiesFileShouldUseDefault() throws Exception {
    assertThat(DistributedSystem.getSecurityPropertiesFile()).isEqualTo(DistributedSystem.SECURITY_PROPERTIES_FILE_DEFAULT);
  }
  
  @Test
  public void getSecurityPropertiesFileShouldUseSystemProperty() throws Exception {
    String propertiesFileName = "testsecurity.properties";
    System.setProperty(DistributedSystem.SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFileName);
    
    assertThat(DistributedSystem.getSecurityPropertiesFile()).isEqualTo(propertiesFileName);
  }
  
  @Test
  public void getSecurityPropertiesFileShouldUseSystemPropertyPath() throws Exception {
    String propertiesFileName = "/home/testsecurity.properties";
    System.setProperty(DistributedSystem.SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFileName);
    
    assertThat(DistributedSystem.getSecurityPropertiesFile()).isEqualTo(propertiesFileName);
  }
}
