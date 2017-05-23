/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.protocol.client


import org.apache.geode.cache.Cache
import org.apache.geode.cache.Region
import org.apache.geode.protocol.protobuf.ClientProtocol
import org.apache.geode.test.junit.categories.UnitTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.mockito.Mockito.verify
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream


@Category(UnitTest::class)
class ProtobufSerializationDeserializationTest {

    private lateinit var mockCache: Cache
    private lateinit var mockRegion: Region<*, *>
    private val testRegion = "testRegion"
    private val testKey = "testKey"
    private val testValue = "testValue"

    @Before
    fun start() {
        mockCache = Mockito.mock(Cache::class.java)
        mockRegion = Mockito.mock(Region::class.java)
        `when`(mockCache.getRegion<Any, Any>(testRegion)).thenReturn(mockRegion as Region<Any, Any>)

    }

    /**
     * Given a serialized message that we've built, verify that the server part does the right call to
     * the Cache it gets passed.
     */
    @Test
    @Throws(IOException::class)
    fun testNewClientProtocolPutsOnPutMessage() {
        val message = MessageUtils
                .makePutMessageFor(region = testRegion, key = testKey, value = testValue)

        val mockOutputStream = Mockito.mock(OutputStream::class.java)

        val newClientProtocol = ProtobufProtocolMessageHandler()
        newClientProtocol.receiveMessage(MessageUtils.loadMessageIntoInputStream(message),
                mockOutputStream, mockCache)

        verify(mockRegion as Region<Any, Any>).put(testKey, testValue)
    }

    @Test
    @Throws(IOException::class)
    fun testServerRespondsToPutMessage() {
        val outputStream = ByteArrayOutputStream(128)
        val message = MessageUtils
                .makePutMessageFor(region = testRegion, key = testKey, value = testValue)

        val newClientProtocol = ProtobufProtocolMessageHandler()
        newClientProtocol.receiveMessage(MessageUtils.loadMessageIntoInputStream(message), outputStream,
                mockCache)

        val responseMessage = ClientProtocol.Message
                .parseDelimitedFrom(ByteArrayInputStream(outputStream.toByteArray()))

        assertEquals(responseMessage.messageTypeCase,
                ClientProtocol.Message.MessageTypeCase.RESPONSE)
        assertEquals(responseMessage.response.responseAPICase,
                ClientProtocol.Response.ResponseAPICase.PUTRESPONSE)
        assertTrue(responseMessage.response.putResponse.success)
    }
}

