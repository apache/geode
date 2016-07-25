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
package com.gemstone.gemfire.test.dunit;

import com.gemstone.gemfire.internal.util.DebuggerSupport;

/**
 * <code>DebuggerUtils</code> provides static utility methods that facilitate
 * runtime debugging.
 * 
 * These methods can be used directly: <code>DebuggerUtils.attachDebugger(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.DebuggerUtils.*;
 *    ...
 *    attachDebugger(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 * 
 * @see com.gemstone.gemfire.internal.util.DebuggerSupport
 */
public class DebuggerUtils {

  protected DebuggerUtils() {
  }
  
  @SuppressWarnings("serial")
  public static void attachDebugger(final VM vm, final String message) {
    vm.invoke(new SerializableRunnable(DebuggerSupport.class.getSimpleName()+" waitForJavaDebugger") {
      public void run() {
        DebuggerSupport.waitForJavaDebugger(message);
      } 
    });
  }

}
