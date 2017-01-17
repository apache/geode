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
package javaobject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Properties;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;

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
