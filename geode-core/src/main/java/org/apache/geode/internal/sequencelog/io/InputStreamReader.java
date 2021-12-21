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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.internal.sequencelog.GraphType;
import org.apache.geode.internal.sequencelog.model.GraphReaderCallback;

public class InputStreamReader {

  private final DataInputStream input;

  public InputStreamReader(InputStream stream) {
    input = new DataInputStream(new BufferedInputStream(stream));
  }

  public void addToGraphs(GraphReaderCallback set, Filter filter) throws IOException {
    List<String> strings = new ArrayList<>();

    while (true) {
      byte recordType = (byte) input.read();
      switch (recordType) {
        case OutputStreamAppender.STRING_RECORD:
          strings.add(input.readUTF());
          break;
        case OutputStreamAppender.EDGE_RECORD:
          long timestamp = input.readLong();
          GraphType graphType = GraphType.getType(input.readByte());
          boolean isPattern = input.readBoolean();
          String graphName = null;
          Pattern graphPattern = null;
          if (isPattern) {
            String pattern = input.readUTF();
            int flags = input.readInt();
            graphPattern = Pattern.compile(pattern, flags);
          } else {
            graphName = input.readUTF();
            // TODO - canonicalize this on write,
            graphName = graphName.intern();
          }
          String stateName = input.readUTF();
          // TODO - canonicalize this on write,
          stateName = stateName.intern();
          String edgeName = readCanonicalString(strings);
          String source = readCanonicalString(strings);
          String dest = readCanonicalString(strings);

          if (isPattern) {
            if (filter.acceptPattern(graphType, graphPattern, edgeName, source, dest)) {
              set.addEdgePattern(timestamp, graphType, graphPattern, edgeName, stateName, source,
                  dest);
            }
          } else {
            if (filter.accept(graphType, graphName, edgeName, source, dest)) {
              set.addEdge(timestamp, graphType, graphName, edgeName, stateName, source, dest);
            }
          }
          break;
        case -1:
          // end of file
          return;
        default:
          throw new IOException("Unknown record type " + recordType);

      }
    }

  }

  private String readCanonicalString(List<String> strings) throws IOException {
    int index = input.readInt();
    if (index == -1) {
      return null;
    }
    return strings.get(index);
  }

}
