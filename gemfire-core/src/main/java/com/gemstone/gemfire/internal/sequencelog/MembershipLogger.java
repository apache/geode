/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

import java.util.regex.Pattern;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author dsmith
 *
 */
public class MembershipLogger {
  
  private static final SequenceLogger GRAPH_LOGGER = SequenceLoggerImpl.getInstance();
  private static final Pattern ALL = Pattern.compile(".*");
  
  public static void logCrash(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.REGION, ALL, "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.KEY, ALL, "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.MESSAGE, ALL, "crash", "destroyed", member, member);
  }
  
  public static void logStartup(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "start", "running", member, member);
  }
  
  public static void logShutdown(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "stop", "destroyed", member, member);
  }

}
