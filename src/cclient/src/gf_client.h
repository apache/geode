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

#ifndef __C_GEMFIRE_CLIENT_H__
#define __C_GEMFIRE_CLIENT_H__

#include <stdint.h>

typedef struct 
{
    int32_t sockfd;
    int64_t eidSeq;
} CONTEXT;


enum error_codes {
    NO_ERROR,
    CONNECTION_ERROR = 1,
    HANDSHAKE_ERROR,
    OPERATION_ERROR
};

/*
 * Establish a connection with the specified GemFire endpoint.
 *
 * @param host the hostname to connect.
 * @param port the port to connect.
 * @param resultcode the result code returned to the caller.
 * @returns a pointer to the context required for further operations.
 */

CONTEXT* gf_connect(char * host, char* port, int32_t * resultcode);

/*
 * Close down a connection previously established with a GemFire endpoint.
 *
 * @param CONTEXT the context of the connection to close.
 * @param resultcode the result code returned to the caller.
 */

void gf_disconnect(CONTEXT * context, int32_t * resultcode);

/*
 * Store data associated with the specified UUID into the GemFire system.
 * Callee does not free the data.
 *
 * @param CONTEXT the context of the connection to use.
 * @param region the GemFire region where the data goes.
 * @param uuid the UUID key associated with the data.
 * @param data a pointer to the data to store.
 * @param len the byte length of the data.
 * @param resultcode the result code returned to the caller.
 */

void gf_put(CONTEXT * context, const char * uuid, const int8_t * data, int32_t len, int32_t * resultcode);

/*
 * Read data associated with the specified UUID from GemFire.
 * Caller must free the returned data.
 *
 * @param CONTEXT the context of the connection to use.
 * @param region the GemFire region from where the data is retrieved.
 * @param uuid the UUID key associated with the data.
 * @param len the byte length of the data being returned.
 * @param resultcode the result code returned to the caller.
 */

void gf_get(CONTEXT * context, const char * uuid, int8_t* data, uint32_t len, int32_t * resultcode);

/*
 * Destroy the data associated with the specified UUID key from the GemFire system.
 *
 * @param CONTEXT the context of the connection to use.
 * @param region the GemFire region from where the data is cleared.
 * @param uuid the UUID key associated with the data.
 * @param resultcode the result code returned to the caller.
 */

void gf_destroy(CONTEXT * context, const char * uuid, int32_t * resultcode);

/*
 * Send a ping message to the server 
 *
 * @param CONTEXT the context of the connection to use.
 * @param resultcode the result code returned to the caller.
 */

void gf_ping(CONTEXT* context, int32_t* resultcode);

#endif

