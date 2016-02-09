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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Exposes GEODE-537: NPE in JTA AFTER_COMPLETION command processing
 */
@Category(UnitTest.class)
public class CommitCommandTest {

	/**
	 * Test for GEODE-537
	 * No NPE should be thrown from the {@link CommitCommand#writeCommitResponse(com.gemstone.gemfire.internal.cache.TXCommitMessage, Message, ServerConnection)}
	 * if the response message is null as it is the case when JTA
	 * transaction is rolled back with TX_SYNCHRONIZATION AFTER_COMPLETION STATUS_ROLLEDBACK 
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testWriteNullResponse() throws IOException {
		
		Cache cache = mock(Cache.class);
		Message origMsg = mock(Message.class);
		ServerConnection servConn = mock(ServerConnection.class);
		when(servConn.getResponseMessage()).thenReturn(mock(Message.class));
		when(servConn.getCache()).thenReturn(cache);
		when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
		
		CommitCommand.writeCommitResponse(null, origMsg, servConn);
		
	}
	
}
