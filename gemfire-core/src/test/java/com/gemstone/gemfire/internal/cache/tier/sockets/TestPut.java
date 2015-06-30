/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
 package com.gemstone.gemfire.internal.cache.tier.sockets;
 
import java.io.IOException;

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;

/**
 * @author Pallavi
 * 
 * TestPut is a dummy command to verify Command handling in BackwardCompatibilityComamndDUnitTest.
 */

 public class TestPut implements Command {

   public TestPut() {	
   }
			  
   final public void execute(Message msg, ServerConnection servConn) {
	 // set flag true - to be checked in test
	 BackwardCompatibilityCommandDUnitDisabledTest.TEST_PUT_COMMAND_INVOKED = true;
	 
	 // write reply to clients 
	 servConn.setAsTrue(REQUIRES_RESPONSE);
	 writeReply(msg, servConn);
	 servConn.setAsTrue(RESPONDED);	 
   }

   private void writeReply(Message origMsg, ServerConnection servConn) {
     Message replyMsg = servConn.getReplyMessage();
     replyMsg.setMessageType(MessageType.REPLY);
     replyMsg.setNumberOfParts(1);
     replyMsg.setTransactionId(origMsg.getTransactionId());
     replyMsg.addBytesPart(BaseCommand.OK_BYTES);
     try {
       replyMsg.send();
     } 
     catch (IOException ioe){
       ioe.printStackTrace();
     }
   }
 }

