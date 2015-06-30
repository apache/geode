/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * This interface provides both {@link Serializable} and {@link
 * Callable}.  It is often used in conjunction with {@link
 * VM#invoke(Callable)}.
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
public abstract class SerializableCallable<T> implements Callable<T>, Serializable {
  
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
