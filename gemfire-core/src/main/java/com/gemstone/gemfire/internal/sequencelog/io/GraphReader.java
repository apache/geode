/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.sequencelog.GraphType;
import com.gemstone.gemfire.internal.sequencelog.model.GraphSet;

/**
 * @author dsmith
 *
 */
public class GraphReader {
  
  private File[] files;

  public GraphReader(File file) {
    this (new File[] {file});
  }
  
  public GraphReader(File[] files) {
    this.files = files;
  }
  
  public GraphSet readGraphs() throws IOException {
    return readGraphs(false);
  }
  
  public GraphSet readGraphs(boolean areGemfireLogs) throws IOException {
    return readGraphs(new Filter() {
      public boolean accept(GraphType graphType, String name, String edgeName,
          String source, String dest) {
        return true;
      }

      public boolean acceptPattern(GraphType graphType, Pattern pattern,
          String edgeName, String source, String dest) {
        return true;
      }
    }, areGemfireLogs);
  }
  
  public GraphSet readGraphs(Filter filter)
  throws IOException {
    return readGraphs(filter, false);
  }
  
  public GraphSet readGraphs(Filter filter, boolean areGemfireLogs)
  throws IOException {
    GraphSet graphs = new GraphSet();
    
    if(areGemfireLogs) {
      //TODO - probably don't need to go all the way
      //to a binary format here, but this is quick and easy.
      HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
      GemfireLogConverter.convertFiles(out, files);
      InputStreamReader reader = new InputStreamReader(out.getInputStream());
      reader.addToGraphs(graphs, filter);
    }
    else {
      for(File file : files) {
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(fis);
        reader.addToGraphs(graphs, filter);
      }
    }
    graphs.readingDone();
    return graphs;
  }
}
