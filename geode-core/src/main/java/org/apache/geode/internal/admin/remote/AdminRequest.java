/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;

/**
 * A message that is sent to a particular distribution manager to make an administration request.
 */
public abstract class AdminRequest extends PooledDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  private String modifiedClasspath = "";
  protected transient String friendlyName = "";


  // instance variables

  /**
   * The reply procesor used to gathering replies to an AdminRequest. See bug 31562.
   */
  private transient AdminReplyProcessor processor;

  /** The (reply processor) id of this message */
  protected int msgId;

  // static methods

  // constructors
  public AdminRequest() {

  }

  // instance methods

  public int getMsgId() {
    return this.msgId;
  }


  /**
   * Sends this request, waits for the AdminReponse, and returns it
   */
  public AdminResponse sendAndWait(ClusterDistributionManager dm) {
    InternalDistributedMember recipient = this.getRecipient();
    if (dm.getId().equals(recipient)) {
      // We're sending this message to ourselves, we won't need a
      // reply process. Besides, if we try to create one, we'll get
      // an assertion failure.
      this.msgId = -1;

    } else {
      this.processor = new AdminReplyProcessor(dm.getSystem(), recipient);
      this.msgId = this.processor.getProcessorId();
    }

    return AdminWaiters.sendAndWait(this, dm);
  }

  /**
   * Waits a given number of milliseconds for the reply to this request.
   *
   * @return Whether or not a reply was received.
   *
   * @see #getResponse
   */
  boolean waitForResponse(long timeout) throws InterruptedException {
    // if (Thread.interrupted()) throw new InterruptedException(); not necessary waitForReplies does
    // this?
    try {
      return this.processor.waitForReplies(timeout);

    } catch (ReplyException ex) {
      for (Throwable cause = ex.getCause(); cause != null; cause = cause.getCause()) {
        if (cause instanceof RuntimeAdminException) {
          throw (RuntimeAdminException) cause;
        }
      }

      throw new RuntimeAdminException(
          "A ReplyException was thrown while waiting for a reply.",
          ex);
    }
  }

  /**
   * Returns the response to this <code>AdminRequest</code>.
   *
   * @see AdminReplyProcessor#getResponse
   */
  AdminResponse getResponse() {
    return this.processor.getResponse();
  }

  /**
   * This method is invoked on the receiver side. It creates a response message and puts it on the
   * outgoing queue.
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    AdminResponse response = null;
    InspectionClasspathManager cpMgr = InspectionClasspathManager.getInstance();
    try {
      cpMgr.jumpToModifiedClassLoader(modifiedClasspath);
      response = createResponse(dm);
    } catch (Exception ex) {
      response = AdminFailureResponse.create(this.getSender(), ex);
    } finally {
      cpMgr.revertToOldClassLoader();
    }
    if (response != null) { // cancellations result in null response
      response.setMsgId(this.getMsgId());
      dm.putOutgoing(response);
    } else {
      logger.info("Response to  {}  was cancelled.", this.getClass().getName());
    }
  }

  /**
   * Must return a proper response to this request.
   */
  protected abstract AdminResponse createResponse(DistributionManager dm);

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.msgId);
    DataSerializer.writeString(this.modifiedClasspath, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.msgId = in.readInt();
    this.modifiedClasspath = DataSerializer.readString(in);
  }

  public void setModifiedClasspath(String path) {
    if (path == null) {
      this.modifiedClasspath = "";
    } else {
      this.modifiedClasspath = path;
    }
  }

  public InternalDistributedMember getRecipient() {
    InternalDistributedMember[] recipients = getRecipients();
    int size = recipients.length;
    if (size == 0) {
      return null;
    } else if (size > 1) {
      throw new IllegalStateException(
          String.format("Could not return one recipient because this message has %s recipients",
              Integer.valueOf(size)));
    } else {
      return recipients[0];
    }
  }

}
