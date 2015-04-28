package com.gemstone.org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface VersionedStreamable extends Streamable {

  /** returns the versions where on-wire changes have been made to the class */
  short[] getSerializationVersions();
  
  /** DataSerialization toData method */
  void toData(DataOutput out) throws IOException;
  
  /** DataSerialization fromData method */
  void fromData(DataInput in) throws IOException, ClassNotFoundException;
  
}
