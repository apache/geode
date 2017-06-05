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
import org.apache.geode.cache.Cache
import org.apache.geode.cache.CacheWriterException
import org.apache.geode.cache.TimeoutException
import org.apache.geode.distributed.LeaseExpiredException
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler
import org.apache.geode.internal.logging.LogService
import org.apache.geode.protocol.protobuf.BasicTypes
import org.apache.geode.protocol.protobuf.ClientProtocol.*
import org.apache.geode.protocol.protobuf.RegionAPI
import org.apache.geode.protocol.protobuf.RegionAPI.*
import org.apache.geode.protocol.protobuf.ServerAPI
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

class ProtobufProtocolMessageHandler : ClientProtocolMessageHandler {

    private fun ErrorMessageFromMessage(message: Message): String {
        return "Error parsing message, message string: " + message.toString()
    }

    @Throws(IOException::class)
    override fun receiveMessage(inputStream: InputStream, outputStream: OutputStream, cache: Cache) {
        val message = Message.parseDelimitedFrom(inputStream) ?: return
        // can be null at EOF, see Parser.parseDelimitedFrom(java.io.InputStream)

        if (message.messageTypeCase != Message.MessageTypeCase.REQUEST) {
            TODO("Got message of type response: ${ErrorMessageFromMessage(message)}")
        }

        val request = message.request
        val requestAPICase = request.requestAPICase
        when (requestAPICase) {
            Request.RequestAPICase.GETREQUEST -> doGetRequest(request.getRequest, cache).writeDelimitedTo(outputStream)
            Request.RequestAPICase.PUTREQUEST -> doPutRequest(request.putRequest, cache).writeDelimitedTo(outputStream)
            Request.RequestAPICase.DESTROYREGIONREQUEST -> doDestroyRegionRequest(request.destroyRegionRequest, cache).writeDelimitedTo(outputStream)
            Request.RequestAPICase.PINGREQUEST -> doPingRequest(request.pingRequest).writeDelimitedTo(outputStream)
            else -> TODO("The message type: $requestAPICase is not supported")

        }
    }

    private fun doPingRequest(pingRequest: ServerAPI.PingRequest): Message {
        return Message.newBuilder().setResponse(Response.newBuilder().setPingResponse(
                ServerAPI.PingResponse.newBuilder().setSequenceNumber(pingRequest.sequenceNumber)))
                .build()
    }

    private fun doPutRequest(request: PutRequest, cache: Cache): Message {
        val regionName = request.regionName
        val entry = request.entry!!

        val region = cache.getRegion<Any, Any>(regionName)
        try {
            region.put(deserializeEncodedValue(entry.key), deserializeEncodedValue(entry.value))
            return putResponseWithStatus(true)
        } catch (ex: TimeoutException) {
            logger.warn("Caught normal-ish exception doing region put", ex)
            return putResponseWithStatus(false)
        } catch (ex: CacheWriterException) {
            logger.warn("Caught normal-ish exception doing region put", ex)
            return putResponseWithStatus(false)
        }

    }

    private fun doDestroyRegionRequest(request: DestroyRegionRequest, cache: Cache): Message {
        val regionName = request.regionName

        val region = cache.getRegion<Any, Any>(regionName)
        try {
            region.destroyRegion()
        } catch (ex: TimeoutException) {
            logger.warn("Caught normal-ish exception doing region put", ex)
            return destroyResponseWithValue(false)
        } catch (ex: LeaseExpiredException) {
            logger.warn("Caught normal-ish exception doing region put", ex)
            return destroyResponseWithValue(false)
        }
        return destroyResponseWithValue(true)
    }

    private fun destroyResponseWithValue(success: Boolean): Message {
        return Message.newBuilder().setResponse(Response.newBuilder().setDestroyRegionResponse(
                DestroyRegionResponse.newBuilder().setSuccess(success))).build()
    }

    private fun deserializeEncodedValue(encodedValue: BasicTypes.EncodedValue): Any {
        val serializer = EncodingTypeThingy.serializerFromProtoEnum(encodedValue.encodingType)
        return serializer.deserialize(encodedValue.value.toByteArray())
    }

    private fun serializeEncodedValue(encodingType: BasicTypes.EncodingType,
                                      objectToSerialize: Any?): ByteArray? {
        if (objectToSerialize == null) {
            return null // BLECH!!! :(
        }
        val serializer = EncodingTypeThingy.serializerFromProtoEnum(encodingType)
        return serializer.serialize(objectToSerialize)
    }

    private fun putResponseWithStatus(ok: Boolean): Message {
        return Message.newBuilder()
                .setResponse(Response.newBuilder().setPutResponse(PutResponse.newBuilder().setSuccess(ok)))
                .build()
    }

    private fun doGetRequest(request: GetRequest, cache: Cache): Message {
        val region = cache.getRegion<Any, Any>(request.regionName)
        val returnValue = region[deserializeEncodedValue(request.key)]

        if (returnValue == null) {

            return makeGetResponseMessageWithValue(ByteArray(0))
        } else {
            // TODO types in the region?
            return makeGetResponseMessageWithValue(returnValue)
        }
    }

    private fun getEncodingTypeForObject(obj: Any): BasicTypes.EncodingType {
        return EncodingTypeThingy.getEncodingTypeForObjectKT(obj)
    }

    private fun makeGetResponseMessageWithValue(objectToReturn: Any): Message {
        val encodingType = getEncodingTypeForObject(objectToReturn)
        val serializedObject = serializeEncodedValue(encodingType, objectToReturn)

        val encodedValueBuilder = BasicTypes.EncodedValue.newBuilder()
                .setEncodingType(encodingType)
                .setValue(ByteString.copyFrom(serializedObject))
        val getResponseBuilder = RegionAPI.GetResponse.newBuilder().setResult(encodedValueBuilder)
        val responseBuilder = Response.newBuilder().setGetResponse(getResponseBuilder)
        return Message.newBuilder().setResponse(responseBuilder).build()
    }

    companion object {

        private val logger = LogService.getLogger()
    }
}
