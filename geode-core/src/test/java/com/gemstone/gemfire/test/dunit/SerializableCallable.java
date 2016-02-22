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
import java.util.concurrent.Callable;

/**
 * This interface provides both {@link Serializable} and {@link
 * Callable}.  It is often used in conjunction with {@link
 * VM#invoke(SerializableCallableIF)}.
 *
 * <PRE>
 * public void testRepilcatedRegionPut() {
 *   final Host host = Host.getHost(0);
 *   VM vm0 = host.getVM(0);
 *   VM vm1 = host.getVM(1);
 *   final String name = this.getUniqueName();
 *   final Object value = new Integer(42);
 *
 *   SerializableCallable putMethod = new SerializableCallable("Replicated put") {
 *       public Object call() throws Exception {
 *         ...// get replicated test region //...
 *         return region.put(name, value);
 *       }
 *      });
 *   assertNull(vm0.invoke(putMethod));
 *   assertEquals(value, vm1.invoke(putMethod));
 *  }
 * </PRE>
 * 
 * @author Mitch Thomas
 */
public abstract class SerializableCallable<T> implements SerializableCallableIF<T> {
  
  private static final long serialVersionUID = -5914706166172952484L;
  
  private String name;

  public SerializableCallable() {
    this.name = null;
  }

  public SerializableCallable(String name) {
    this.name = name;
  }

  public String toString() {
    if (this.name != null) {
      return "\"" + this.name + "\"";

    } else {
      return super.toString();
    }
  }
}
