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
package com.gemstone.gemfire;

import junit.framework.TestCase;

/**
 * TimingTestCase provides a nanosecond timing framework.  Extend
 * this class instead of TestCase to implement GemFire timing tests.
 */
public abstract class TimingTestCase extends TestCase {

    static private long nanosPerMilli = 1000000;

    public TimingTestCase(String name) {
        super(name);
    }

    /**
     * backward compatibility
     */
    protected void time(String opDescription, int numPasses, int opsPerPass, final Runnable runnable) {
      try {
        time(opDescription, numPasses, opsPerPass, new RunBlock() {
          public void run() {
            runnable.run();
          }});
      } catch (Exception ex) {
        ex.printStackTrace();
        fail(ex.toString());
      }
    }

    /**
     * Invoke the Runnable numPasses times, then compute and display
     * the duration of each operation in nanoseconds.
     * @param opDescription a short description of the operation being timed
     * @param numPasses the number of times to run runnable
     * @param opsPerPass the number of operations occurring in each execution
     *      of runnable.  This is used to compute the time of each operation -
     *      invoking this method will execute the operation numPasses * opsPerPass
     *      times.
     * @param runnable contains the code to execute in the run() method
     */
    protected void time(String opDescription, int numPasses, int opsPerPass, RunBlock runnable) 
    throws Exception {

        // Measure elapsed time to invoke runnable numPasses times
        long start = System.currentTimeMillis();
        for (int pass=numPasses; pass > 0; pass--) {
            runnable.run();
        }
        long elapsed = System.currentTimeMillis() - start;

        // Compute time per operationS
        long nanosPerOp = (long)((float)(elapsed * nanosPerMilli) / (float)(opsPerPass * numPasses));
        System.out.println("Timing " + opDescription + ": " + nanosPerOp + " nanoseconds per operation");
    }
    
    public static interface RunBlock {
      public void run() throws Exception;
    }
}
