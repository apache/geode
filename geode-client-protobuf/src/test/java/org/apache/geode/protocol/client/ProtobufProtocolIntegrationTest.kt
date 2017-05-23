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

import com.pholser.junit.quickcheck.runner.JUnitQuickcheck
import org.apache.geode.cache.Cache
import org.apache.geode.cache.CacheFactory
import org.apache.geode.cache.Region
import org.apache.geode.distributed.ConfigurationProperties
import org.apache.geode.protocol.protobuf.BasicTypes
import org.apache.geode.protocol.protobuf.ClientProtocol.Message.MessageTypeCase.RESPONSE
import org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.GETRESPONSE
import org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.PUTRESPONSE
import org.apache.geode.test.junit.categories.IntegrationTest
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import java.io.IOException
import java.io.Serializable
import java.util.*

@Category(IntegrationTest::class)
@RunWith(JUnitQuickcheck::class)
class ProtobufProtocolIntegrationTest {
    private lateinit var cache: Cache
    private lateinit var testClient: NewClientProtocolTestClient
    private lateinit var regionUnderTest: Region<Any, Any>
    private val testRegion = "testRegion"
    private val testKey = "testKey"
    private val testValue = "testValue"

    @Before
    @Throws(IOException::class)
    fun setup() {
        cache = createCacheOnPort(40404)
        testClient = NewClientProtocolTestClient("localhost", 40404)
        regionUnderTest = cache.createRegionFactory<Any, Any>().create(testRegion)
    }

    @After
    @Throws(IOException::class)
    fun shutdown() {
        testClient.close()
        cache.close()
    }

    @Test
    @Throws(IOException::class)
    fun testRoundTripPutRequest() {
        val message = MessageUtils.makePutMessageFor(region = testRegion, key = testKey, value = testValue)
        val response = testClient.blockingSendMessage(message)
        testClient.printResponse(response)

        assertEquals(RESPONSE, response.messageTypeCase)
        assertEquals(PUTRESPONSE, response.response.responseAPICase)
        assertTrue(response.response.putResponse.success)

        assertEquals(1, regionUnderTest.size.toLong())
        assertTrue(regionUnderTest.containsKey(testKey))
        assertEquals(testValue, regionUnderTest[testKey])
    }

    @Test
    @Throws(IOException::class)
    fun testRoundTripEmptyGetRequest() {
        val message = MessageUtils.makeGetMessageFor(region = testRegion, key = testKey)
        val response = testClient.blockingSendMessage(message)

        assertEquals(RESPONSE, response.messageTypeCase)
        assertEquals(GETRESPONSE, response.response.responseAPICase)
        val value = response.response.getResponse.result

        assertTrue(value.value.isEmpty)
    }

    @Test
    @Throws(IOException::class)
    fun testRoundTripNonEmptyGetRequest() {
        val putMessage = MessageUtils.makePutMessageFor(testRegion, testKey, testValue)
        val putResponse = testClient.blockingSendMessage(putMessage)
        testClient.printResponse(putResponse)

        val getMessage = MessageUtils
                .makeGetMessageFor(testRegion, testKey)
        val getResponse = testClient.blockingSendMessage(getMessage)

        assertEquals(RESPONSE, getResponse.messageTypeCase)
        assertEquals(GETRESPONSE, getResponse.response.responseAPICase)
        val value = getResponse.response.getResponse.result

        assertEquals(value.value.toStringUtf8(), testValue)
    }

    @Test
    fun objectSerializationIntegrationTest() {
        val inputs = listOf("Foobar", 1000.toLong(), 22, 231.toShort(), (-107).toByte(), byteArrayOf(1, 2, 3, 54, 99))
        for (key in inputs) {
            for (value in inputs) {
                if (key !is ByteArray) {
                    testMessagePutAndGet(key, value, EncodingTypeThingy.getEncodingTypeForObjectKT(value))
                }
            }
        }
//        val jsonString = "{ \"_id\": \"5924ba3f3918de8404fc1321\", \"index\": 0, \"guid\": \"bd27d3fa-8870-4f0d-ab4d-73adf7cbe58b\", \"isActive\": false, \"balance\": \"$1,934.31\", \"picture\": \"http://placehold.it/32x32\", \"age\": 39, \"eyeColor\": \"blue\", \"name\": \"Holt Dickson\", \"gender\": \"male\", \"company\": \"INQUALA\", \"email\": \"holtdickson@inquala.com\", \"phone\": \"+1 (886) 450-2949\", \"address\": \"933 Diamond Street, Hinsdale, Palau, 2038\", \"about\": \"Cupidatat excepteur labore cillum ea reprehenderit aliquip magna duis aliquip Lorem labore. Aliquip elit ullamco aliqua fugiat aute id irure enim Lorem eu qui nisi aliquip. Et do sit cupidatat sit ut consectetur ullamco aute do nostrud in. Ea voluptate in reprehenderit sit commodo et aliquip officia id eiusmod. Quis voluptate commodo ad esse do cillum ut occaecat non.\r\n\", \"registered\": \"2017-02-01T12:28:49 +08:00\", \"latitude\": -69.313434, \"longitude\": 134.707471, \"tags\": [ \"officia\", \"qui\", \"ullamco\", \"nostrud\", \"ipsum\", \"dolor\", \"officia\" ], \"friends\": [ { \"id\": 0, \"name\": \"Vivian Beach\" }, { \"id\": 1, \"name\": \"Crystal Mills\" }, { \"id\": 2, \"name\": \"Mosley Frank\" } ], \"greeting\": \"Hello, Holt Dickson! You have 2 unread messages.\", \"favoriteFruit\": \"apple\" }"
//
//        testMessagePutAndGet(testKey,jsonString,BasicTypes.EncodingType.STRING)
//        val putMessage = MessageUtils.makePutMessageFor(region = testRegion, key = testKey, value = jsonString, valueEncoding = BasicTypes.EncodingType.STRING)
    }

    private fun testMessagePutAndGet(key: Serializable, value: Serializable, valueEncoding: BasicTypes.EncodingType) {
        val putMessage = MessageUtils.makePutMessageFor(region = testRegion, key = key, value = value, valueEncoding = valueEncoding)
        val responseMessage = testClient.blockingSendMessage(putMessage)
        assertTrue(responseMessage.response.putResponse.success)

        val getMessage = MessageUtils.makeGetMessageFor(region = testRegion, key = key)
        val getResponse = testClient.blockingSendMessage(getMessage)

        val messageEncodingType = getResponse.response.getResponse.result.encodingType
        assertEquals(valueEncoding, messageEncodingType)

        val serializer = EncodingTypeThingy.serializerFromProtoEnum(messageEncodingType)
        val messageValue = getResponse.response.getResponse.result.value.toByteArray()

        val deserializeValue = serializer.deserializer.deserialize(messageValue)
        when (messageEncodingType) {
            BasicTypes.EncodingType.BINARY -> assertArrayEquals(value as ByteArray, deserializeValue as ByteArray)
            else -> assertEquals(value, serializer.deserializer.deserialize(messageValue))
        }
    }

    @Throws(IOException::class)
    private fun createCacheOnPort(port: Int): Cache {
        val props = Properties()
        props.setProperty(ConfigurationProperties.TCP_PORT, Integer.toString(port))
        props.setProperty(ConfigurationProperties.BIND_ADDRESS, "localhost")
        val cf = CacheFactory(props)
        val cache = cf.create()
        val cacheServer = cache.addCacheServer()
        cacheServer.bindAddress = "localhost"
        cacheServer.start()
        return cache
    }
}
