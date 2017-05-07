/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class FireNForget extends FunctionAdapter  implements
  Declarable {
  public void execute(FunctionContext context) {

    String argument = null;
    ArrayList<String> list = new ArrayList<String>();

    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    String localVM = ds.getDistributedMember().getId();
    //System.out.println(
    //    "Inside function execution (some time) node " + localVM);
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      for (int j = 0; j < 4; j++) {
      }
    }
    list.add(localVM);
    //System.out.println(
    //    "Completed function execution (some time) node " + localVM);
  }

  public String getId() {
    return "FireNForget";
  }

  public boolean hasResult() {
    return false;
  }
  public boolean isHA() {
    return false;
  }

  public void init(Properties props) {
  }

}
