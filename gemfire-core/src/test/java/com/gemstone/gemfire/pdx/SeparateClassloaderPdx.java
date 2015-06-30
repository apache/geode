/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import java.io.IOException;

class SeparateClassloaderPdx implements PdxSerializable {
  
  private final boolean fieldOneFirst;

  public SeparateClassloaderPdx(boolean fieldOneFirst) {
    this.fieldOneFirst = fieldOneFirst;
  }



  public void toData(PdxWriter writer) {
    if(fieldOneFirst) {
      writer.writeString("field1", "hello");
      writer.writeString("field2", "world");
    } else {
      writer.writeString("field2", "world");
      writer.writeString("field1", "hello");
    }
  }

  public void fromData(PdxReader in) {
  }
}