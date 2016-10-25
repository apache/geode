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
package org.apache.geode.modules.session.installer;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.geode.internal.FileUtil;
import org.apache.geode.test.junit.categories.UnitTest;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(UnitTest.class)
public class InstallerJUnitTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void installIntoWebXML() throws Exception {
    testTransformation("InstallerJUnitTest.web.xml");
  }

  private void testTransformation(final String name) throws Exception {
    File webXmlFile = temporaryFolder.newFile();
    FileUtil.copy(getClass().getResource(name), webXmlFile);
    final String[] args = {
      "-t", "peer-to-peer",
      "-w", webXmlFile.getAbsolutePath()
    };

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try(InputStream input = new FileInputStream(webXmlFile)){
      new Installer(args).processWebXml(input, output);
    }

    String expected = IOUtils.toString(getClass().getResource(name + ".expected"));
    assertEquals(expected, output.toString());
  }

}