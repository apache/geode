/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package javaobject;

import java.security.Principal;

public class NoopPrincipal implements Principal {

  public static NoopPrincipal create() {
    return new NoopPrincipal();
  }

  public String getName() {
    return "";
  }
}
