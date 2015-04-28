/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;

/**
 * Utility class to print banner information at manager startup.
 */
public class Banner {

    private Banner() {
	// everything is static so don't allow instance creation
    }
    
    private static void prettyPrintPath(String path, PrintWriter out) {
	if (path != null) {
	    StringTokenizer st =
		new StringTokenizer(path, System.getProperty("path.separator"));
	    while (st.hasMoreTokens()) {
		out.println("  " + st.nextToken());
	    }
	}
    }
    /** 
     * Print information about this process to the specified stream.
     * @param args possibly null list of command line arguments
     */
    private static void print(PrintWriter out, String args[]) {
        Map sp = new TreeMap((Properties)System.getProperties().clone()); // fix for 46822
	int processId = -1;
	final String SEPERATOR = "---------------------------------------------------------------------------";
	try {
	    processId = OSProcess.getId();
	} 
	catch (VirtualMachineError err) {
	  SystemFailure.initiateFailure(err);
	  // If this ever returns, rethrow the error.  We're poisoned
	  // now, so don't let this thread continue.
	  throw err;
	}
	catch (Throwable t) {
	     // Whenever you catch Error or Throwable, you must also
	     // catch VirtualMachineError (see above).  However, there is
	     // _still_ a possibility that you are dealing with a cascading
	     // error condition, so you also need to check to see if the JVM
	     // is still usable:
	     SystemFailure.checkFailure();
	}
	out.println();

        final String productName = GemFireVersion.getProductName();
        
	out.println(SEPERATOR);
	out.println();

        out.println("  Copyright (C) 1997-2015 Pivotal Software, Inc. All rights reserved. This");
        out.println("  product is protected by U.S. and international copyright and intellectual");
        out.println("  property laws. Pivotal products are covered by one or more patents listed");
        out.println("  at http://www.pivotal.io/patents.  Pivotal is a registered trademark");
        out.println("  of trademark of Pivotal Software, Inc. in the United States and/or other");
        out.println("  jurisdictions.  All other marks and names mentioned herein may be");
        out.println("  trademarks of their respective companies.");
        out.println();
        out.println(SEPERATOR);

	GemFireVersion.print(out);

	out.println("Process ID: " + processId);
	out.println("User: " + sp.get("user.name"));
        sp.remove("user.name");
        sp.remove("os.name");
        sp.remove("os.arch");
	out.println("Current dir: " + sp.get("user.dir"));
        sp.remove("user.dir");
	out.println("Home dir: " + sp.get("user.home"));
        sp.remove("user.home");
        List<String> allArgs = new ArrayList<String>();
        {
          RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
          if (runtimeBean != null) {
            allArgs.addAll(runtimeBean.getInputArguments()); // fixes  45353
          }
        }
        
	if (args != null && args.length != 0) {
	    for (int i=0; i < args.length; i++) {
	      allArgs.add(args[i]);
	    }
	}
	if (!allArgs.isEmpty()) {
	  out.println("Command Line Parameters:");
	  for (String arg: allArgs) {
	    out.println("  " + arg);
	  }
	}
	out.println("Class Path:");
	prettyPrintPath((String)sp.get("java.class.path"), out);
        sp.remove("java.class.path");
	out.println("Library Path:");
	prettyPrintPath((String)sp.get("java.library.path"), out);
        sp.remove("java.library.path");

        if (Boolean.getBoolean("gemfire.disableSystemPropertyLogging")) {
          out.println("System property logging disabled.");
        } else {
	out.println("System Properties:");
        Iterator it = sp.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry)it.next();
          String key = me.getKey().toString();
          // SW: Filter out the security properties since they may contain
          // sensitive information.
          if (!key.startsWith(DistributionConfig.GEMFIRE_PREFIX
              + DistributionConfig.SECURITY_PREFIX_NAME)
              && !key.startsWith(DistributionConfigImpl.SECURITY_SYSTEM_PREFIX
                  + DistributionConfig.SECURITY_PREFIX_NAME)
              && !key.toLowerCase().contains("password") /* bug 45381 */) {
            out.println("    " + key + " = " + me.getValue());
          } else {
            out.println("    " + key + " = " + "********");
          }
        }
        }
	out.println(SEPERATOR);
    }

    /**
     * Return a string containing the banner information.
     * @param args possibly null list of command line arguments
     */
    public static String getString(String args[]) {
	StringWriter sw = new StringWriter();
	PrintWriter pw = new PrintWriter(sw);
	print(pw, args);
	pw.close();
	return sw.toString();
    }
}
