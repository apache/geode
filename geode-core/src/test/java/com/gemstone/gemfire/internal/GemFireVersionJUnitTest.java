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

import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.GemFireVersion.VersionDescription;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * This test prints out the version information obtained from the
 * {@link GemFireVersion} class.  It provides a record of what version
 * of GemFire (and the JDK) was used to run the unit tests.
 */
@Category(UnitTest.class)
public class GemFireVersionJUnitTest {

  @Test
  public void testPrintInfo() {
    final String versionOutput = GemFireVersion.asString();
    System.out.println(versionOutput);
    
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.PRODUCT_NAME));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.GEMFIRE_VERSION));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.SOURCE_DATE));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.SOURCE_REVISION));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.SOURCE_REPOSITORY));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.BUILD_DATE));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.BUILD_ID));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.BUILD_PLATFORM));
    assertTrue(versionOutput.contains(GemFireVersion.VersionDescription.BUILD_JAVA_VERSION));
  }
  
  @Test
  public void testNoFile() {
    String noFile = "not a property file";
    VersionDescription noVersion = new VersionDescription(noFile);

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    noVersion.print(pw);

    String noFileOutput = sw.toString();
    assertTrue(noFileOutput.contains(LocalizedStrings.GemFireVersion_COULD_NOT_FIND_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0.toLocalizedString(noFile)));
  }
}
