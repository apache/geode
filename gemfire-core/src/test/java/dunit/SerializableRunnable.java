/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit;

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
  implements Serializable, Runnable {

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
