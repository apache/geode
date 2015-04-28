package com.gemstone.gemfire.distributed.internal.tcpserver;

import com.gemstone.gemfire.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shobhit
 * @since 7.1
 */
public class VersionRequest implements DataSerializable {


  private static final long serialVersionUID = -8272913634136267812L;

  @Override
  public void toData(DataOutput out) throws IOException {
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }
}
