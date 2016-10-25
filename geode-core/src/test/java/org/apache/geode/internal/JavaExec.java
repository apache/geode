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
package org.apache.geode.internal;

import java.io.*;
import java.util.*;

/**
 * Used to exec a java main class in its own vm
 *
 *
 */
public class JavaExec {
    /**
     * Creates a java process that executes the given main class
     * and waits for the process to terminate.
     * @return a {@link ProcessOutputReader} that can be used to
     * get the exit code and stdout+stderr of the terminated process. 
     */
    public static ProcessOutputReader fg(Class main) throws IOException {
      return fg(main, null, null);
    }
    /**
     * Creates a java process that executes the given main class
     * and waits for the process to terminate.
     * @return a {@link ProcessOutputReader} that can be used to
     * get the exit code and stdout+stderr of the terminated process. 
     */
    public static ProcessOutputReader fg(Class main, String[] vmArgs, String[] mainArgs) throws IOException {
	File javabindir = new File(System.getProperty("java.home"), "bin");
	File javaexe = new File(javabindir, "java");

        int bits = Integer.getInteger("sun.arch.data.model", 0).intValue();
        String vmKindArg = (bits == 64) ? "-d64" : null ;

        ArrayList argList = new ArrayList();
        argList.add(javaexe.getPath());
        if (vmKindArg != null) {
          argList.add(vmKindArg);
        }
        //argList.add("-Dgemfire.systemDirectory=" + GemFireConnectionFactory.getDefaultSystemDirectory());
        argList.add("-Djava.class.path=" + System.getProperty("java.class.path"));
        argList.add("-Djava.library.path=" + System.getProperty("java.library.path"));
        if (vmArgs != null) {
          argList.addAll(Arrays.asList(vmArgs));
        }
        argList.add(main.getName());
        if (mainArgs != null) {
          argList.addAll(Arrays.asList(mainArgs));
        }
        String[] cmd = (String[])argList.toArray(new String[argList.size()]);
	return new ProcessOutputReader(Runtime.getRuntime().exec(cmd));
    }
}
