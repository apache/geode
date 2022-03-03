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
package org.apache.geode.internal.sequencelog.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.sequencelog.GraphType;
import org.apache.geode.internal.sequencelog.model.GraphSet;
import org.apache.geode.internal.serialization.KnownVersion;

public class GraphReader {

  private final File[] files;

  public GraphReader(File file) {
    this(new File[] {file});
  }

  public GraphReader(File[] files) {
    this.files = files;
  }

  public GraphSet readGraphs() throws IOException {
    return readGraphs(false);
  }

  public GraphSet readGraphs(boolean areGemfireLogs) throws IOException {
    return readGraphs(new Filter() {
      @Override
      public boolean accept(GraphType graphType, String name, String edgeName, String source,
          String dest) {
        return true;
      }

      @Override
      public boolean acceptPattern(GraphType graphType, Pattern pattern, String edgeName,
          String source, String dest) {
        return true;
      }
    }, areGemfireLogs);
  }

  public GraphSet readGraphs(Filter filter) throws IOException {
    return readGraphs(filter, false);
  }

  public GraphSet readGraphs(Filter filter, boolean areGemfireLogs) throws IOException {
    GraphSet graphs = new GraphSet();

    if (areGemfireLogs) {
      // TODO - probably don't need to go all the way
      // to a binary format here, but this is quick and easy.
      try (HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT)) {
        GemfireLogConverter.convertFiles(out, files);
        InputStreamReader reader = new InputStreamReader(out.getInputStream());
        reader.addToGraphs(graphs, filter);
      }
    } else {
      for (File file : files) {
        try (FileInputStream fis = new FileInputStream(file)) {
          InputStreamReader reader = new InputStreamReader(fis);
          reader.addToGraphs(graphs, filter);
        }
      }
    }
    graphs.readingDone();
    return graphs;
  }
}
