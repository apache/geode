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
package org.apache.geode.internal.tcp;

//import java.io.*;
import java.util.*;

import org.apache.geode.GemFireCheckedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * This exception is thrown as a result of one or more failed attempts
 * to connect to a remote conduit.
 *
 *
 *
 * @since GemFire 3.0
 */
public class ConnectExceptions extends GemFireCheckedException {
private static final long serialVersionUID = -4173688946448867706L;

  /** The causes of this exception */
  private List causes;

  /** The InternalDistributedMember's of the members we couldn't connect/send to */
  private List members;
  

  ////////////////////  Constructors  ////////////////////

  /**
   * Creates a new <code>ConnectExceptions</code>
   */
  public ConnectExceptions() {
    super(LocalizedStrings.ConnectException_COULD_NOT_CONNECT.toLocalizedString());
    this.causes = new ArrayList();
    this.members = new ArrayList();
  }


  /**
   * Notes the member we couldn't connect to.
   */
  public void addFailure(InternalDistributedMember member, Throwable cause) {
    this.members.add(member);
    this.causes.add(cause);
  }

  /**
   * Returns a list of <code>InternalDistributedMember</code>s that couldn't be connected
   * to.
   */
  public List getMembers() {
    return this.members;
  }

  /**
   * Returns the causes of this exception
   */
  public List getCauses() {
    return this.causes;
  }

  @Override
  public String getMessage() {
    StringBuffer sb = new StringBuffer();
    for (Iterator iter = this.members.iterator(); iter.hasNext(); ) {
      sb.append(' ').append(iter.next());
    }
    sb.append(" ").append(LocalizedStrings.ConnectException_CAUSES.toLocalizedString());
    for (Iterator iter = this.causes.iterator(); iter.hasNext(); ) {
      sb.append(" {").append(iter.next()).append("}");
    }
    return LocalizedStrings.ConnectException_COULD_NOT_CONNECT_TO_0.toLocalizedString(sb);
  }

}
