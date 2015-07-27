package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;

public class ViewAckMessage extends DistributionMessage {

  int viewId;
  NetView alternateView;
  
  public ViewAckMessage(InternalDistributedMember recipient, int viewId) {
    super();
    setRecipient(recipient);
    this.viewId = viewId;
  }
  
  public ViewAckMessage(InternalDistributedMember recipient, NetView alternateView) {
    super();
    setRecipient(recipient);
    this.alternateView = alternateView;
  }
  
  public ViewAckMessage() {
    // no-arg constructor for serialization
  }
  
  public int getViewId() {
    return viewId;
  }
  
  @Override
  public int getDSFID() {
    // TODO Auto-generated method stub
    return VIEW_ACK_MESSAGE;
  }

  @Override
  public int getProcessorType() {
    return 0;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.viewId);
    DataSerializer.writeObject(this.alternateView, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.viewId = in.readInt();
    this.alternateView = DataSerializer.readObject(in);
  }
  
  @Override
  public String toString() {
    String s = getSender() == null? getRecipientsDescription() : ""+getSender();
    return "ViewAckMessage("+s+"; "+this.viewId+"; altview="+this.alternateView+")";
  }

}
