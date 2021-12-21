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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.regex.Pattern;

import org.apache.geode.internal.sequencelog.Transition;

/**
 * Appends events logged using the SequenceLogger to a binary stream.
 *
 */
public class OutputStreamAppender {
  private final IdentityHashMap<Object, Integer> writtenObjects =
      new IdentityHashMap<>();
  private final HashMap<String, Integer> writtenStrings = new HashMap<>();
  private final DataOutputStream outputStream;

  private int nextInt = 0;

  public static final byte EDGE_RECORD = 0x01;
  public static final byte STRING_RECORD = 0x02;

  public OutputStreamAppender(File file) throws FileNotFoundException {
    this(new FileOutputStream(file));
  }

  public OutputStreamAppender(OutputStream out) throws FileNotFoundException {
    outputStream = new DataOutputStream(new BufferedOutputStream(out, 256));
    writtenObjects.put(null, Integer.valueOf(-1));
  }

  public void write(Transition edge) throws IOException {
    byte graphType = edge.getType().getId();
    // TODO - really we should deal with null and read it back in as null as well.
    String stateName = edge.getState() == null ? "null" : edge.getState().toString();
    long timestamp = edge.getTimestamp();
    int edgeId = canonalize(edge.getEdgeName());
    int source = canonalize(edge.getSource());
    int dest = canonalize(edge.getDest());

    outputStream.write(EDGE_RECORD);
    outputStream.writeLong(timestamp);
    outputStream.write(graphType);
    writeGraphName(edge.getGraphName());
    outputStream.writeUTF(stateName);
    outputStream.writeInt(edgeId);
    outputStream.writeInt(source);
    outputStream.writeInt(dest);
    outputStream.flush();
  }

  private void writeGraphName(Object graphName) throws IOException {
    boolean isPattern = graphName instanceof Pattern;
    outputStream.writeBoolean(isPattern);
    if (isPattern) {
      final Pattern pattern = (Pattern) graphName;
      outputStream.writeUTF(pattern.pattern());
      outputStream.writeInt(pattern.flags());
    } else {
      outputStream.writeUTF(graphName.toString());
    }
  }

  private int canonalize(Object object) throws IOException {
    Integer id = writtenObjects.get(object);
    if (id != null) {
      return id.intValue();
    }
    String toString = object.toString();
    id = writtenStrings.get(toString);
    if (id != null) {
      return id.intValue();
    }

    id = Integer.valueOf(nextInt++);

    outputStream.write(STRING_RECORD);
    outputStream.writeUTF(toString);

    writtenObjects.put(object, id);
    writtenStrings.put(toString, id);
    return id.intValue();
  }

  public void close() {
    try {
      outputStream.close();
    } catch (IOException e) {
      // do nothing
    }
  }
}
