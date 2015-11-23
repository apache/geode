package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class GetViewResponse implements DataSerializableFixedID {

  private NetView view;

  public GetViewResponse(NetView view) {
    this.view = view;
  }
  
  public GetViewResponse() {
    // no-arg constructor for serialization
  }
  
  public NetView getView() {
    return view;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return GET_VIEW_RESP;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(view, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    view = DataSerializer.readObject(in);
  }

}
