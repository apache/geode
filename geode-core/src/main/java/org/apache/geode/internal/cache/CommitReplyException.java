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
package org.apache.geode.internal.cache;

import java.util.Collections;
import java.util.Set;

import org.apache.geode.distributed.internal.ReplyException;

/**
 * Contains exceptions generated when attempting to process a commit operation.
 *
 * @since GemFire 5.0
 */
public class CommitReplyException extends ReplyException {
  private static final long serialVersionUID = -7711083075296622596L;

  /** Exceptions generated when attempting to process a commit operation */
  private final Set<Exception> exceptions;

  /**
   * Constructs a <code>CommitReplyException</code> with a message.
   *
   * @param s the String message
   */
  public CommitReplyException(String s) {
    super(s);
    exceptions = Collections.emptySet();
  }

  /**
   * Constructs a <code>CommitReplyException</code> with a message and set of exceptions generated
   * when attempting to process a commit operation.
   *
   * @param s the String message
   * @param exceptions set of exceptions generated when attempting to process a commit operation
   */
  public CommitReplyException(String s, Set<Exception> exceptions) {
    super(s);
    this.exceptions = exceptions;
  }

  /**
   * Returns set of exceptions generated when attempting to process a commit operation
   *
   * @return set of exceptions generated when attempting to process a commit operation
   */
  public Set<Exception> getExceptions() {
    return exceptions;
  }

  @Override
  public String toString() {
    return super.toString() + " with exceptions: " + exceptions;
  }
}
