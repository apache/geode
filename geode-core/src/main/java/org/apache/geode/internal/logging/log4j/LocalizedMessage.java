/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;

import org.apache.logging.log4j.message.Message;

import org.apache.geode.i18n.StringId;


/**
 * An implementation of a Log4j {@link Message} that wraps a {@link StringId}.
 * 
 */
public final class LocalizedMessage implements Message {
  private static final long serialVersionUID = -8893339995741536401L;

  private final StringId stringId;
  private final Object[] params;
  private final Throwable throwable;
  
  private LocalizedMessage(final StringId stringId, final Object[] params, final Throwable throwable) {
    this.stringId = stringId;
    this.params = params;
    this.throwable = throwable;
  }

  public static LocalizedMessage create(final StringId stringId) {
    return new LocalizedMessage(stringId, null, null);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object[] params) {
    return new LocalizedMessage(stringId, params, null);
  }

  public static final LocalizedMessage create(final StringId stringId, final Throwable throwable) {
    return new LocalizedMessage(stringId, null, throwable);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object object) {
    return new LocalizedMessage(stringId, new Object[] { object }, null);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object[] params, final Throwable throwable) {
    return new LocalizedMessage(stringId, params, throwable);
  }
  
  @Override
  public String getFormattedMessage() {
    return this.stringId.toLocalizedString(params);
  }

  @Override
  public String getFormat() {
    return this.stringId.getRawText();
  }

  @Override
  public Object[] getParameters() {
    return this.params;
  }

  @Override
  public Throwable getThrowable() {
    return this.throwable;
  }
}
