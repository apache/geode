/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.*;
import java.util.*;

/**
 * Used to exec a java main class in its own vm
 *
 * @author darrel
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
