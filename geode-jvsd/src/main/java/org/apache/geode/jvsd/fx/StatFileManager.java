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
package org.apache.geode.jvsd.fx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.jvsd.model.stats.StatArchiveFile;

/**
 * Basic container for all stat files we are managing.
 *
 * @author Jens Deppe
 */
public class StatFileManager {

  private List<StatArchiveFile> statFiles = new ArrayList<>();

  private static StatFileManager instance = new StatFileManager();

  private StatFileManager() {
    // We are a singleton
  }

  public static StatFileManager getInstance() {
    return instance;
  }

  public void add(String[] fileNames) throws IOException {
    for (String name : fileNames) {
      statFiles.add(new StatArchiveFile(name));
    }
  }
  
  public void add(String fileName) throws IOException {
      statFiles.add(new StatArchiveFile(fileName));
  }

  public List<StatArchiveFile> getArchives() {
    return statFiles;
  }
}
