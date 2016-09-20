/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class JMXProperties extends Properties {
  private static final long serialVersionUID = -6210901350494570026L;

  private static JMXProperties props = new JMXProperties();

  public static JMXProperties getInstance() {
    return props;
  }

  public void load(String propFile) throws IOException {
    if (propFile != null) {
      FileInputStream fin;
      fin = new FileInputStream(new File(propFile));
      if (fin != null) {
        clear();
        load(fin);
      }

      fin.close();
    }
  }
}
