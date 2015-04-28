/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.sequencelog.GraphType;
import com.gemstone.gemfire.internal.sequencelog.model.Graph;
import com.gemstone.gemfire.internal.sequencelog.model.GraphReaderCallback;

/**
 * @author dsmith
 *
 */
public class InputStreamReader {
  
  private DataInputStream input;

  public InputStreamReader(InputStream stream) {
    this.input = new DataInputStream(new BufferedInputStream(stream));
  }
  
  public void addToGraphs(GraphReaderCallback set, Filter filter) throws IOException {
    List<String> strings = new ArrayList<String>();
    
    while(true) {
      byte recordType = (byte) input.read();
      switch(recordType) {
        case OutputStreamAppender.STRING_RECORD:
          strings.add(input.readUTF());
          break;
        case OutputStreamAppender.EDGE_RECORD:
          long timestamp = input.readLong();
          GraphType graphType = GraphType.getType(input.readByte());
          boolean isPattern = input.readBoolean();
          String graphName = null;
          Pattern graphPattern = null;
          if(isPattern) {
            String pattern = input.readUTF();
            int flags = input.readInt();
            graphPattern = Pattern.compile(pattern, flags);
          } else {
            graphName = input.readUTF();
            //TODO - canonicalize this on write,
            graphName = graphName.intern();
          }
          String stateName = input.readUTF();
          //TODO - canonicalize this on write, 
          stateName = stateName.intern();
          String edgeName  = readCanonicalString(strings);
          String source = readCanonicalString(strings);
          String dest = readCanonicalString(strings);
          
          if(isPattern) {
            if(filter.acceptPattern(graphType, graphPattern, edgeName, source, dest)) {
              set.addEdgePattern(timestamp, graphType, graphPattern, edgeName, stateName, source, dest);
            }
          } else { 
            if(filter.accept(graphType, graphName, edgeName, source, dest)) {
              set.addEdge(timestamp, graphType, graphName, edgeName, stateName, source, dest);
            }
          }
          break;
        case -1:
          //end of file
          return;
        default:
          throw new IOException("Unknown record type " + recordType);
      
      }
    }
    
  }
  
  private String readCanonicalString(List<String> strings) throws IOException {
    int index = input.readInt();
    if(index == -1) {
      return null;
    }
    return strings.get(index);
  }

}
