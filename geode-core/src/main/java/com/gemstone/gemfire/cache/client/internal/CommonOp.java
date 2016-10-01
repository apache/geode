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

package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;

import java.io.IOException;

/**
 * Attempts to refactor some duplicate code in project
 * Created by zhouwei on 2016/10/1.
 *
 */
public abstract class CommonOp {
    /**
     * Process response exception happeded
     * @throws Exception if response could not be processed or
     * we received a response with a server exception.
     */
    protected void processResponseException(final int msgType, ChunkedMessage msg) throws IOException {
        if (isErrorResponse(msgType)) {
            msg.receiveChunk();
            Part part = msg.getPart(0);
            throw new ServerOperationException(part.getString());
        } else {
            throw new InternalGemFireError("Unexpected message type "
                    + MessageType.getString(msgType));
        }
    }

    /**
     * Attempts to read a response to this operation by reading it from the
     * given connection, and returning it.
     * @return the result of the operation
     *         or <code>null</code> if the operation has no result.
     * @throws Exception if the execute failed
     */
    protected Object readResponse(Connection cnx) throws Exception {
        Message message = createResponseMessage();
        if (message != null) {
            message.setComms(cnx.getSocket(), cnx.getInputStream(),
                    cnx.getOutputStream(), cnx.getCommBuffer(), cnx.getStats());
            if (message instanceof ChunkedMessage) {
                try {
                    return processResponse(message, cnx);
                } finally {
                    message.unsetComms();
                    processSecureBytes(cnx, message);
                }
            } else {
                try {
                    message.recv();
                } finally {
                    message.unsetComms();
                    processSecureBytes(cnx, message);
                }
                return processResponse(message, cnx);
            }
        } else {
            return null;
        }
    }

    /**
     * Processes the given response message returning the result, if any,
     * of the processing.
     * @return the result of processing the response; null if no result
     * @throws Exception if response could not be processed or
     * we received a response with a server exception.
     */
    protected abstract Object processResponse(Message message, Connection connection) throws Exception;

    /**
     * Return true of <code>msgType</code> indicates the operation
     * had an error on the server.
     */
    protected abstract boolean isErrorResponse(int msgType);

    protected abstract void processSecureBytes(Connection connection, Message message) throws Exception;

    protected abstract Message createResponseMessage();
}
