/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;

/**
 * Holds a dependency between one object and another. This class is used as an
 * edge in the @link {@link DependencyGraph}.
 * 
 * An example of a dependency is a Thread that is blocking on a lock, or a lock
 * that is held by a thread.
 * 
 * @author dsmith
 * 
 */
public class Dependency<A,B> implements Serializable {
  
  private static final long serialVersionUID = 1L;
  
  public final A depender;
  public final B dependsOn;
  
  public Dependency(A depender, B dependsOn) {
    this.depender = depender;
    this.dependsOn = dependsOn;
  }
  
  public A getDepender() {
    return depender;
  }
  
  public B getDependsOn() {
    return dependsOn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((depender == null) ? 0 : depender.hashCode());
    result = prime * result + ((dependsOn == null) ? 0 : dependsOn.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Dependency))
      return false;
    Dependency<?,?> other = (Dependency<?,?>) obj;
    if (depender == null) {
      if (other.depender != null)
        return false;
    } else if (!depender.equals(other.depender))
      return false;
    if (dependsOn == null) {
      if (other.dependsOn != null)
        return false;
    } else if (!dependsOn.equals(other.dependsOn))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return depender  + " -> " + dependsOn;
  }
  
}

