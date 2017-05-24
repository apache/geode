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

import com.google.protobuf.ByteString
import org.apache.geode.protocol.protobuf.BasicTypes
import org.apache.geode.protocol.protobuf.ClientProtocol
import org.apache.geode.protocol.protobuf.RegionAPI
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*

object MessageUtils {
    @Throws(IOException::class)
    fun loadMessageIntoInputStream(message: ClientProtocol.Message): ByteArrayInputStream {
        val byteArrayOutputStream = ByteArrayOutputStream()
        message.writeDelimitedTo(byteArrayOutputStream)
        val messageByteArray = byteArrayOutputStream.toByteArray()
        return ByteArrayInputStream(messageByteArray)
    }

    fun makeGetMessageFor(region: String, key: Any, keyEncoding: BasicTypes.EncodingType = EncodingTypeThingy.getEncodingTypeForObjectKT(key)): ClientProtocol.Message {
        val random = Random()
        val messageHeader = ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt())

        val keyBuilder = getEncodedValueBuilder(key, keyEncoding)

        val getRequest = RegionAPI.GetRequest.newBuilder().setRegionName(region).setKey(keyBuilder)
        val request = ClientProtocol.Request.newBuilder().setGetRequest(getRequest)

        return ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)
                .build()
    }

    fun makePutMessageFor(region: String, key: Any, value: Any, keyEncoding: BasicTypes.EncodingType = EncodingTypeThingy.getEncodingTypeForObjectKT(key), valueEncoding: BasicTypes.EncodingType = EncodingTypeThingy.getEncodingTypeForObjectKT(value)): ClientProtocol.Message {
        val random = Random()
        val messageHeader = ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt())

        val keyBuilder = getEncodedValueBuilder(key, keyEncoding)
        val valueBuilder = getEncodedValueBuilder(value, valueEncoding)

        val putRequestBuilder = RegionAPI.PutRequest.newBuilder().setRegionName(region)
                .setEntry(BasicTypes.Entry.newBuilder().setKey(keyBuilder).setValue(valueBuilder))

        val request = ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder)
        val message = ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)

        return message.build()
    }

    fun makePutMessageForJSON(region: String, key: Any, value: String, keyEncoding: BasicTypes.EncodingType = EncodingTypeThingy.getEncodingTypeForObjectKT(key)): ClientProtocol.Message {
        val random = Random()
        val messageHeader = ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt())

        val keyBuilder = getEncodedValueBuilder(key, keyEncoding)
        val valueBuilder = getEncodedValueBuilder(value, BasicTypes.EncodingType.STRING)
        valueBuilder.encodingType = BasicTypes.EncodingType.JSON


        val putRequestBuilder = RegionAPI.PutRequest.newBuilder().setRegionName(region)
                .setEntry(BasicTypes.Entry.newBuilder().setKey(keyBuilder).setValue(valueBuilder))

        val request = ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder)
        val message = ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)

        return message.build()
    }

    private fun getEncodedValueBuilder(value: Any, encodingType: BasicTypes.EncodingType): BasicTypes.EncodedValue.Builder {
        return BasicTypes.EncodedValue.newBuilder().setEncodingType(encodingType)
                .setValue(ByteString.copyFrom(EncodingTypeThingy.serializerFromProtoEnum(encodingType).serialize(value)))
    }
}
