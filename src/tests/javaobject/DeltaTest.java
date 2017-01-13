/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.Instantiator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

public class DeltaTest implements DataSerializable, Delta, Declarable {

  private int _deltaUpdate = 0;

  private String _staticData = "static data";

  static {
     Instantiator.register(new Instantiator(DeltaTest.class, 151) {
     public DataSerializable newInstance() {
        return new DeltaTest();
     }
   });
  }

  public boolean hasDelta( )
  {
    return _deltaUpdate%2 == 1;
  }

  public void fromData( DataInput in ) throws IOException
  {
    _deltaUpdate = in.readInt( );
    _staticData = in.readUTF();
  }

  public void toData( DataOutput out ) throws IOException
  {
    out.writeInt( _deltaUpdate );
    out.writeUTF(_staticData);
  }

  public void fromDelta( DataInput in ) throws IOException
  {
    _deltaUpdate = in.readInt( );
  }

  public void toDelta( DataOutput out ) throws IOException
  {
    out.writeInt( _deltaUpdate );
  }

  public void init( Properties props )
  {
    _deltaUpdate = 0;
  }

};
