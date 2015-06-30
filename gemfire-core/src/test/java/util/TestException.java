/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package util;

import java.lang.Error;

public class TestException extends Error {

  public TestException(String message, Throwable cause) {
    super(message, cause);
  }

public TestException(String name) {
   super(name);
}

public TestException(StringBuffer name) {
   super(name.toString());
}

}
