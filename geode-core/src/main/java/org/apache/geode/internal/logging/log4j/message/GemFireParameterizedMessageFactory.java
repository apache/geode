/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache license, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the license for the specific language governing permissions and limitations under
 * the license.
 */
package org.apache.geode.internal.logging.log4j.message;

import org.apache.logging.log4j.message.AbstractMessageFactory;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.ParameterizedMessage;

/**
 * Enables use of <code>{}</code> parameter markers in message strings.
 * <p/>
 * Creates {@link ParameterizedMessage} instances for {@link #newMessage(String, Object...)}.
 * <p/>
 * This class is immutable.
 * <p/>
 * Copied into Geode from org.apache.logging.log4j.message.ParameterizedMessageFactory
 * (http://logging.apache.org/log4j/2.x/license.html)
 * <p/>
 * Geode changes include changing class name and package. Additional changes are commented with "//
 * GEODE: note"
 */
public final class GemFireParameterizedMessageFactory extends AbstractMessageFactory {

  private static final long serialVersionUID = 1L;

  /**
   * Instance of StringFormatterMessageFactory.
   */
  public static final GemFireParameterizedMessageFactory INSTANCE =
      new GemFireParameterizedMessageFactory();

  /**
   * Creates {@link ParameterizedMessage} instances.
   * 
   * @param message The message pattern.
   * @param params The message parameters.
   * @return The Message.
   *
   * @see MessageFactory#newMessage(String, Object...)
   */
  @Override
  public Message newMessage(final String message, final Object... params) {
    return new GemFireParameterizedMessage(message, params); // GEODE: change to construct
                                                             // GemFireParameterizedMessage
  }
}
