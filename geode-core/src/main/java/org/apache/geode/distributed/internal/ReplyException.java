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

package org.apache.geode.distributed.internal;

import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SerializationException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Exception thrown when a DistributionMessage is processed to be propagated back to the sender of
 * the message.
 *
 */
public class ReplyException extends GemFireException {
  private static final long serialVersionUID = -4410839793809166071L;
  private static final String REMOTE_MEMBER_TOKEN = "Remote Member";
  private transient InternalDistributedMember sender;

  /**
   * Creates a new instance of <code>ReplyException</code> without detail message.
   *
   * Used by serialization
   */
  public ReplyException() {}


  /**
   * Constructs an instance of <code>ReplyException</code> with the specified detail message.
   *
   * @param msg the detail message.
   */
  public ReplyException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>ReplyException</code> with the specified detail message and
   * cause.
   *
   * @param msg the detail message.
   * @param cause the causal Throwable
   */
  public ReplyException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>ReplyException</code> with the specified cause.
   *
   * @param cause the causal Throwable
   */
  public ReplyException(Throwable cause) {
    super(cause);
  }

  /**
   * Before calling this method any expected "checked" causes should be handled by the caller. If
   * cause is null or a checked exception (that is not ClassNotFound) then throw an
   * InternalGemFireException because those should have already been handled. Otherwise
   * ClassNotFoundException will be handled by throwing SerializationException. RuntimeException and
   * Error will have their stack fixed up and then are thrown.
   */
  public void handleCause() {
    Throwable c = getCause();
    if (c == null) {
      throw new InternalGemFireException(
          String.format("unexpected exception on member %s",
              getSender()),
          this);
    }
    if (c instanceof RuntimeException) {
      fixUpRemoteEx(c);
      throw (RuntimeException) c;
    }
    if (c instanceof Error) {
      fixUpRemoteEx(c);
      throw (Error) c;
    }
    if (c instanceof ClassNotFoundException) {
      // for bug 43602
      throw new SerializationException("Class not found", c);
    }
    throw new InternalGemFireException(
        String.format("unexpected exception on member %s",
            getSender()),
        c);
  }

  /**
   * Fixes a remote exception that this ReplyException has wrapped. Adds the local stack frames. The
   * remote stack elements have the sender id info.
   *
   * @param t Remote exception to fix up
   * @since GemFire 5.1
   */
  private void fixUpRemoteEx(Throwable t) {
    if (getSender() == null) {
      return;
    }

    String senderId = getSender().toString();
    addSenderInfo(t, senderId);

    StackTraceElement[] remoteStack = t.getStackTrace();
    StackTraceElement[] localStack = Thread.currentThread().getStackTrace();

    int localStartIdx = 0;
    if (localStartIdx < localStack.length) {
      // pop off the fixUpRemoteEx frame
      localStartIdx++;
      if (localStartIdx < localStack.length) {
        // pop off the handleAsUnexpected frame
        localStartIdx++;
      }
    }

    // do not consider localStartIdx no. of elements.
    StackTraceElement[] newStack =
        new StackTraceElement[remoteStack.length + localStack.length - localStartIdx];

    int i = 0;
    for (; i < remoteStack.length; i++) {
      newStack[i] = remoteStack[i];
    }
    for (int j = 2; i < newStack.length; j++, i++) {
      newStack[i] = localStack[j];
    }

    t.setStackTrace(newStack);
  }

  /**
   * Adds the sender information to the stack trace elements of the given exception. Also traverses
   * recursively over the 'cause' for adding this sender information.
   *
   * @param toModify Throwable instance to modify the stack trace elements
   * @param senderId id of the sender member
   */
  private static void addSenderInfo(Throwable toModify, String senderId) {
    StackTraceElement[] stackTrace = toModify.getStackTrace();

    StackTraceElement element = null;
    for (int i = 0; i < stackTrace.length; i++) {
      element = stackTrace[i];
      if (!element.getClassName().startsWith(REMOTE_MEMBER_TOKEN)) {
        stackTrace[i] = new StackTraceElement(
            REMOTE_MEMBER_TOKEN + " '" + senderId + "' in " + element.getClassName(),
            element.getMethodName(), element.getFileName(), element.getLineNumber());
      }
    }

    toModify.setStackTrace(stackTrace);
    Throwable cause = toModify.getCause();

    if (cause != null) {
      addSenderInfo(cause, senderId);
    }
  }

  /**
   * Sets the member that threw the received exception
   *
   * @param sendr the member that threw the exception
   * @since GemFire 6.0
   */
  public synchronized void setSenderIfNull(InternalDistributedMember sendr) {
    if (this.sender == null) {
      this.sender = sendr;
    }
  }

  /**
   * Gets the member which threw the exception
   *
   * @return the throwing member
   * @since GemFire 6.0
   */
  public synchronized InternalDistributedMember getSender() {
    return this.sender;
  }

  @Override
  public String getMessage() {
    InternalDistributedMember s = getSender();
    String m = super.getMessage();
    return (s != null) ? ("From " + s + ": " + m) : m;
  }
}
