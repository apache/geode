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
package org.apache.geode.internal.size;

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
