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
package org.apache.geode.internal.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;

public class Configuration {

  private URL resource;
  private String configFileName;

  public Configuration(final URL resource, final String configFileName) {
    this.resource = resource;
    this.configFileName = configFileName;
  }

  public File createConfigFileIn(final File targetFolder) throws IOException, URISyntaxException {
    File targetFile = new File(targetFolder, this.configFileName);
    IOUtils.copy(this.resource.openStream(), new FileOutputStream(targetFile));
    assertThat(targetFile).hasSameContentAs(new File(this.resource.toURI()));
    return targetFile;
  }
}
