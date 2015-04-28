/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.client.internal.ExecuteFunctionOp;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.internal.FunctionServiceManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.MemberFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.ServerToClientFunctionResultSender;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * 
 * @author sbawaska
 */
public class ExecuteFunction70 extends ExecuteFunction66 {

  private static final ExecuteFunction70 singleton = new ExecuteFunction70();

  public static Command getCommand() {
    return singleton;
  }
  
  private ExecuteFunction70() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    super.cmdExecute(msg, servConn, start);
  }
  
  @Override
  protected String[] getGroups(Message msg) throws IOException, ClassNotFoundException {
    String[] grp = null;
    Part p = msg.getPart(4);
    if (p != null) {
      grp = (String[]) p.getObject();
    }
    return grp;
  }

  @Override
  protected boolean getIgnoreFailedMembers(Message msg) {
    return isFlagSet(msg, ExecuteFunctionOp.IGNORE_FAILED_MEMBERS_INDEX);
  }

  @Override
  protected boolean getAllMembers(Message msg) {
    return isFlagSet(msg, ExecuteFunctionOp.ALL_MEMBERS_INDEX);
  }

  private boolean isFlagSet(Message msg, int index) {
    boolean isSet = false;
    byte[] flags = null;
    Part p = msg.getPart(5);
    if (p != null) {
      flags = p.getSerializedForm();
      if (flags != null && flags.length > index) {
        if (flags[index] == 1) {
          isSet = true;
        }
      }
    }
    return isSet;
  }

  @Override
  protected void executeFunctionOnGroups(Object function, Object args,
      String[] groups, boolean allMembers, Function functionObject,
      ServerToClientFunctionResultSender resultSender, boolean ignoreFailedMembers) {

    DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds == null) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_DS_NOT_CREATED_OR_NOT_READY
              .toLocalizedString());
    }
    Set<DistributedMember> members = new HashSet<DistributedMember>();
    for (String group : groups) {
      if (allMembers) {
        members.addAll(ds.getGroupMembers(group));
      } else {
        ArrayList<DistributedMember> memberList = new ArrayList<DistributedMember>(ds.getGroupMembers(group));
        if (!memberList.isEmpty()) {
          if (!FunctionServiceManager.RANDOM_onMember && memberList.contains(ds.getDistributedMember())) {
            members.add(ds.getDistributedMember());
          } else {
            Collections.shuffle(memberList);
            members.add(memberList.get(0));
          }
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Executing Function on Groups: {} all members: {} members are: {}", Arrays.toString(groups), allMembers, members);
    }
    Execution execution = new MemberFunctionExecutor(ds, members, resultSender);
    if (args != null) {
      execution = execution.withArgs(args);
    }
    if (ignoreFailedMembers) {
      if (logger.isDebugEnabled()) {
        logger.debug("Function will ignore failed members");
      }
      ((AbstractExecution)execution).setIgnoreDepartedMembers(true);
    }
    if (function instanceof String) {
      execution.execute(functionObject.getId()).getResult();
    } else {
      execution.execute(functionObject).getResult();
    }
  }
}
