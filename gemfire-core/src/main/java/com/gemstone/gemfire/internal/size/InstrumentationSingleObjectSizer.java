/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import java.lang.instrument.Instrumentation;


public class InstrumentationSingleObjectSizer implements SingleObjectSizer {
  
  private static Instrumentation instrumentation;
  
  public static void premain(String agentArgs, Instrumentation instrumentation) {
    InstrumentationSingleObjectSizer.instrumentation = instrumentation;
  }
  
  public InstrumentationSingleObjectSizer() {
    if(instrumentation == null) {
      throw new RuntimeException("SizeOfUtil50 must be used as an instrumentation agent by specifying -javaagent:/path/to/sizeagent.jar to java on the command line");
    }
    
  }

  public long sizeof(Object object) {
    return instrumentation.getObjectSize(object);
  }
  

}
