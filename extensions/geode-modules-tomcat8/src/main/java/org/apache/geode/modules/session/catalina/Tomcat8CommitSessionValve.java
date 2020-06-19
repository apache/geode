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

package org.apache.geode.modules.session.catalina;

import java.lang.reflect.Field;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.coyote.OutputBuffer;

public class Tomcat8CommitSessionValve
    extends AbstractCommitSessionValve<Tomcat8CommitSessionValve> {

  private static final Field outputBufferField;

  static {
    try {
      outputBufferField = org.apache.coyote.Response.class.getDeclaredField("outputBuffer");
      outputBufferField.setAccessible(true);
    } catch (final NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  Response wrapResponse(final Response response) {
    final org.apache.coyote.Response coyoteResponse = response.getCoyoteResponse();
    final OutputBuffer delegateOutputBuffer = getOutputBuffer(coyoteResponse);
    if (!(delegateOutputBuffer instanceof Tomcat8CommitSessionOutputBuffer)) {
      final Request request = response.getRequest();
      final OutputBuffer sessionCommitOutputBuffer =
          new Tomcat8CommitSessionOutputBuffer(() -> commitSession(request), delegateOutputBuffer);
      coyoteResponse.setOutputBuffer(sessionCommitOutputBuffer);
    }
    return response;
  }

  static OutputBuffer getOutputBuffer(final org.apache.coyote.Response coyoteResponse) {
    try {
      return (OutputBuffer) outputBufferField.get(coyoteResponse);
    } catch (final IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

}
