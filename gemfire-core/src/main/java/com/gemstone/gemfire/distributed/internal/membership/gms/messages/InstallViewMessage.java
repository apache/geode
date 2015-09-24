package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;

public class InstallViewMessage extends HighPriorityDistributionMessage {

  private NetView view;
  private Object credentials;
  private boolean preparing;

  public InstallViewMessage(NetView view, Object credentials) {
    this.view = view;
    this.preparing = false;
    this.credentials = credentials;
  }

  public InstallViewMessage(NetView view, Object credentials, boolean preparing) {
    this.view = view;
    this.preparing = preparing;
    this.credentials = credentials;
  }
  
  public InstallViewMessage() {
    // no-arg constructor for serialization
  }

  public NetView getView() {
    return view;
  }

  public Object getCredentials() {
    return credentials;
  }

  public boolean isPreparing() {
    return preparing;
  }

  @Override
  public int getDSFID() {
    return INSTALL_VIEW_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.view, out);
    DataSerializer.writeObject(this.credentials, out);
    out.writeBoolean(preparing);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.view = DataSerializer.readObject(in);
    this.credentials = DataSerializer.readObject(in);
    this.preparing = in.readBoolean();
  }

  @Override
  public String toString() {
    return "InstallViewMessage(preparing="+this.preparing+"; "+this.view
            +"; cred="+(credentials==null?"null": "not null")
             +")";
  }

}
