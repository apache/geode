package com.gemstone.gemfire.test.process;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

/**
 * Provides a main which delegates to another main for testing after waiting
 * for one input. The purpose is to prevent race condition in which a process
 * may send output before the reader has started listening for that output. 
 * 
 * @author Kirk Lund
 */
public class MainLauncher {
  public static void main(String... args) throws Exception {
    assert args.length > 0;
    String innerMain = args[0];
    Class<?> clazz = Class.forName(innerMain);
    
    //System.out.println(MainLauncher.class.getSimpleName() + " waiting to start...");
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    bufferedReader.readLine();
    
    //System.out.println(MainLauncher.class.getSimpleName() + " delegating...");
    Object[] innerArgs = new String[args.length - 1];
    for (int i = 0; i < innerArgs.length; i++) {
      innerArgs[i] = args[i + 1];
    }
    Method mainMethod = clazz.getMethod("main", String[].class);
    mainMethod.invoke(null, new Object[] { innerArgs });
  }
}
