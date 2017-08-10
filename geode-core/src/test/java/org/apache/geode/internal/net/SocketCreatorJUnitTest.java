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
package org.apache.geode.internal.net;

import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.util.test.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class SocketCreatorJUnitTest {

  @Test
  public void testCreateSocketCreatorWithKeystoreUnset() throws Exception {
    SSLConfig testSSLConfig = new SSLConfig();
    testSSLConfig.setEnabled(true);
    testSSLConfig.setKeystore(null);
    testSSLConfig.setKeystorePassword("");
    testSSLConfig.setTruststore(getSingleKeyKeystore());
    testSSLConfig.setTruststorePassword("password");
    // GEODE-3393: This would fail with java.io.FileNotFoundException: $USER_HOME/.keystore
    new SocketCreator(testSSLConfig);

  }

  private String getSingleKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore");
  }

}
