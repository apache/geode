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
package org.apache.geode.internal.sequencelog.visualization.text;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.geode.internal.sequencelog.GraphType;
import org.apache.geode.internal.sequencelog.io.Filter;
import org.apache.geode.internal.sequencelog.io.InputStreamReader;
import org.apache.geode.internal.sequencelog.model.GraphReaderCallback;

public class TextDisplay {

  public static void main(String[] args) throws IOException {
    File[] files;
    if (args.length > 0) {
      files = new File[args.length];
      for (int i = 0; i < args.length; i++) {
        files[i] = new File(args[i]);
      }
    } else {
      files = new File[] {new File("states.graph")};
    }

    for (File file : files) {
      System.out.println("FILE: " + file);
      try (FileInputStream fileInputStream = new FileInputStream(file);
          BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
        InputStreamReader reader = new InputStreamReader(bufferedInputStream);
        reader.addToGraphs(new GraphReaderCallback() {

          @Override
          public void addEdge(long timestamp, GraphType graphType, String graphName,
              String edgeName,
              String state, String source, String dest) {
            System.out.println(timestamp + ": (" + graphType + ", " + graphName + ", " + edgeName
                + ", " + state + ", " + source + ", " + dest + ")");

          }

          @Override
          public void addEdgePattern(long timestamp, GraphType graphType, Pattern graphNamePattern,
              String edgeName, String state, String source, String dest) {
            System.out.println(timestamp + ": (" + graphType + ", " + graphNamePattern + ", "
                + edgeName + ", " + state + ", " + source + ", " + dest + ")");
          }
        }, new Filter() {

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

        });
      }

    }

  }

}
