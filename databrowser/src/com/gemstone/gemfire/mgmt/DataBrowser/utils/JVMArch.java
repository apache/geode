/*
 *  =========================================================================
 *  Copyright (c) 2002-2012 VMware, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. VMware products are covered by
 *  more patents listed at http://www.vmware.com/go/patents.
 *  All Rights Reserved.
 *  ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.utils;

/**
 * Reads Java System Property 'os.arch' & prints it on console.
 *   
 * @author abhishek
 */
public class JVMArch {  
  public static void main(String[] args) {
    String osArch       = "x86";
    String osArchActual = System.getProperty("os.arch");
//    System.out.println(osArchActual); // only for testing
    
    // The idea to detect the JVM Architecture/Data Model is taken from 
    // org.eclipse.equinox.launcher.Main.getArch()
    if (osArchActual.equalsIgnoreCase("i386")) {
      osArch = "x86";
    } else if (osArchActual.equalsIgnoreCase("amd64")) {
      osArch = "x86_64";
    }
    System.out.println(osArch);
  }
}
