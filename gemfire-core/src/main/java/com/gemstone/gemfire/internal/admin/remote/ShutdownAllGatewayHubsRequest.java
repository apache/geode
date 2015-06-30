package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;

/**
 * 
 * This ShutdownAllGatewayHubsRequest just reply with ignored bit true so that
 * old version member's request will be ignored and no exception will be thrown.
 * 
 * From 9.0 old wan support is removed. Ideally ShutdownAllGatewayHubsRequest
 * should be removed but it it there for rolling upgrade support when request
 * come from old version member to shut down hubs.
 * 
 * @author kbachhav
 * @since 9.0
 *
 */
public class ShutdownAllGatewayHubsRequest extends DistributionMessage {
  
  protected int rpid;

  @Override
  public int getDSFID() {
    return SHUTDOWN_ALL_GATEWAYHUBS_REQUEST;
  }

  @Override
  public int getProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }

  @Override
  protected void process(DistributionManager dm) {
    ReplyMessage.send(getSender(), this.rpid, null, dm, true /*ignored*/, false, false);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.rpid = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.rpid);
  }
}
