/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.visualization.text;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.sequencelog.GraphType;
import com.gemstone.gemfire.internal.sequencelog.io.Filter;
import com.gemstone.gemfire.internal.sequencelog.io.InputStreamReader;
import com.gemstone.gemfire.internal.sequencelog.model.GraphReaderCallback;

/**
 * @author dsmith
 *
 */
public class TextDisplay {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    File[] files;
    if(args.length > 0) {
      files = new File[args.length];
      for(int i = 0; i < args.length; i++) {
        files[i] = new File(args[i]);
      }
    } else {
      files = new File[] {new File("states.graph")};
    }
    
    for(File file: files) {
      System.out.println("FILE: " + file);
      InputStreamReader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(file)));
      reader.addToGraphs(new GraphReaderCallback() {
        
        public void addEdge(long timestamp, GraphType graphType, String graphName,
            String edgeName, String state, String source, String dest) {
          System.out.println(timestamp + ": (" + graphType + ", " + graphName + ", " + edgeName + ", " + state + ", " + source + ", " + dest + ")");
          
        }

        public void addEdgePattern(long timestamp, GraphType graphType,
            Pattern graphNamePattern, String edgeName, String state,
            String source, String dest) {
          System.out.println(timestamp + ": (" + graphType + ", " + graphNamePattern + ", " + edgeName + ", " + state + ", " + source + ", " + dest + ")");
        }
      }, new Filter() {

        public boolean accept(GraphType graphType, String name,
            String edgeName, String source, String dest) {
          return true;
        }

        public boolean acceptPattern(GraphType graphType, Pattern pattern,
            String edgeName, String source, String dest) {
          return true;
        }
        
      });
      
    }

  }

}
