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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Method;
import java.util.Map;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class AbstractConfigJUnitTest {

  @Test
  public void testDisplayPropertyValue() throws Exception {
    AbstractConfigTestClass actc = new AbstractConfigTestClass();
    Method method = actc.getClass().getSuperclass().getDeclaredMethod("okToDisplayPropertyValue", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(actc, "password"));
    assertFalse((Boolean) method.invoke(actc,CLUSTER_SSL_TRUSTSTORE_PASSWORD));
    assertTrue((Boolean) method.invoke(actc, CLUSTER_SSL_ENABLED));
    assertFalse((Boolean) method.invoke(actc, GATEWAY_SSL_TRUSTSTORE_PASSWORD));
    assertFalse((Boolean) method.invoke(actc, SERVER_SSL_KEYSTORE_PASSWORD));
    assertTrue((Boolean) method.invoke(actc, SSL_ENABLED));
    assertTrue((Boolean) method.invoke(actc, CONSERVE_SOCKETS));
    assertFalse((Boolean) method.invoke(actc, "javax.net.ssl.keyStorePassword"));
    assertFalse((Boolean) method.invoke(actc, "javax.net.ssl.keyStoreType"));
    assertFalse((Boolean) method.invoke(actc, "sysprop-value"));
  }

  private static class AbstractConfigTestClass extends AbstractConfig {

    @Override
    protected Map getAttDescMap() {
      return null;
    }

    @Override
    protected Map<String, ConfigSource> getAttSourceMap() {
      return null;
    }

    @Override
    public Object getAttributeObject(String attName) {
      return null;
    }

    @Override
    public void setAttributeObject(String attName, Object attValue, ConfigSource source) {

    }

    @Override
    public boolean isAttributeModifiable(String attName) {
      return false;
    }

    @Override
    public Class getAttributeType(String attName) {
      return null;
    }

    @Override
    public String[] getAttributeNames() {
      return new String[0];
    }

    @Override
    public String[] getSpecificAttributeNames() {
      return new String[0];
    }
  }
}
