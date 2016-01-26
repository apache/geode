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

import java.io.Serializable;

/**
 * This interface provides both {@link Serializable} and {@link
 * Runnable}.  It is often used in conjunction with {@link
 * VM#invoke(Runnable)}.
 *
 * <PRE>
 * public void testRegionPutGet() {
 *   final Host host = Host.getHost(0);
 *   VM vm0 = host.getVM(0);
 *   VM vm1 = host.getVM(1);
 *   final String name = this.getUniqueName();
 *   final Object value = new Integer(42);
 *
 *   vm0.invoke(new SerializableRunnable("Put value") {
 *       public void run() {
 *         ...// get the region //...
 *         region.put(name, value);
 *       }
 *      });
 *   vm1.invoke(new SerializableRunnable("Get value") {
 *       public void run() {
 *         ...// get the region //...
 *         assertEquals(value, region.get(name));
 *       }
 *     });
 *  }
 * </PRE>
 */
public abstract class SerializableRunnable
  implements SerializableRunnableIF {

  private static final long serialVersionUID = 7584289978241650456L;
  
  private String name;

  public SerializableRunnable() {
    this.name = null;
  }

  /**
   * This constructor lets you do the following:
   *
   * <PRE>
   * vm.invoke(new SerializableRunnable("Do some work") {
   *     public void run() {
   *       // ...
   *     }
   *   });
   * </PRE>
   */
  public SerializableRunnable(String name) {
    this.name = name;
  }
  
  public void setName(String newName) {
    this.name = newName;
  }
  
  public String getName() {
    return this.name;
  }

  public String toString() {
    if (this.name != null) {
      return "\"" + this.name + "\"";

    } else {
      return super.toString();
    }
  }

}
