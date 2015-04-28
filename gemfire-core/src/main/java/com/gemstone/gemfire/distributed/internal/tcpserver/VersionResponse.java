package com.gemstone.gemfire.distributed.internal.tcpserver;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.Version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Get GemFire version of the member running TcpServer.
 * @author shobhit
 * @since 7.1
 */
public class VersionResponse implements DataSerializable {


  private static final long serialVersionUID = 8320323031808601748L;
  private short versionOrdinal = Version.TOKEN.ordinal();

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeShort(versionOrdinal);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    versionOrdinal = in.readShort();
  }

  public short getVersionOrdinal() {
    return versionOrdinal;
  }

  public void setVersionOrdinal(short versionOrdinal) {
    this.versionOrdinal = versionOrdinal;
  }
}
