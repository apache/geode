/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

//import java.io.*;
import java.util.*;

import com.gemstone.gemfire.GemFireCheckedException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This exception is thrown as a result of one or more failed attempts
 * to connect to a remote conduit.
 *
 * @author David Whitlock
 *
 *
 * @since 3.0
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
