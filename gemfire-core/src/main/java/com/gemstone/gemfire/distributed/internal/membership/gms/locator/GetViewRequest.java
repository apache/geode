package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class GetViewRequest implements DataSerializableFixedID, PeerLocatorRequest {

  public GetViewRequest() {
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return GET_VIEW_REQ;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

}
