/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.io;

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

import com.gemstone.gemfire.internal.sequencelog.Transition;

/**
 * Appends events logged using the SequenceLogger to a binary stream.
 * @author dsmith
 *
 */
public class OutputStreamAppender {
  private IdentityHashMap<Object, Integer> writtenObjects = new IdentityHashMap<Object, Integer>();
  private HashMap<String, Integer> writtenStrings = new HashMap<String, Integer>();
  private DataOutputStream outputStream;
  
  private int nextInt = 0;
  
  public static final byte EDGE_RECORD = 0x01;
  public static final byte STRING_RECORD = 0x02;

  public OutputStreamAppender(File file) throws FileNotFoundException {
    this(new FileOutputStream(file));
  }
  
  public OutputStreamAppender(OutputStream out) throws FileNotFoundException {
    this.outputStream = new DataOutputStream(new BufferedOutputStream(
        out, 256));
    writtenObjects.put(null, Integer.valueOf(-1));
  }
  
  public void write(Transition edge) throws IOException {
    byte graphType =edge.getType().getId();
    //TODO - really we should deal with null and read it back in as null as well.
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
    if(isPattern) {
      final Pattern pattern = (Pattern) graphName;
      outputStream.writeUTF(pattern.pattern());
      outputStream.writeInt(pattern.flags());
    } else {
      outputStream.writeUTF(graphName.toString());
    }
  }

  private int canonalize(Object object) throws IOException {
    Integer id = writtenObjects.get(object);
    if(id !=null) {
      return id.intValue();
    }
    String toString = object.toString();
    id = writtenStrings.get(toString);
    if(id !=null) {
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
    } catch(IOException e) {
      //do nothing
    }
  }
}
