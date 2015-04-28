/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import jline.ConsoleReader;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

public class CLIConsoleBufferUtil {
  public static String processMessegeForExtraCharactersFromConsoleBuffer(String messege){
    
    ConsoleReader reader = Gfsh.getConsoleReader();    
    if (reader != null) {
      StringBuffer buffer = reader.getCursorBuffer().getBuffer();    
      if(buffer.length() > messege.length()){
        int appendSpaces = buffer.length() - messege.length();
        for(int i = 0; i < appendSpaces; i++){
          messege = messege + " ";
        }
      }   
    }
    return messege;
  }
}
