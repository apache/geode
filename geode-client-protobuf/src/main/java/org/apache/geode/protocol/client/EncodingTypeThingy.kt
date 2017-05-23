package org.apache.geode.protocol.client

import org.apache.geode.pdx.JSONFormatter
import org.apache.geode.pdx.PdxInstance
import org.apache.geode.protocol.protobuf.BasicTypes
import org.apache.geode.serialization.SerializationType

object EncodingTypeThingy {
    @JvmStatic
    fun getEncodingTypeForObjectKT(obj: Any?): BasicTypes.EncodingType {
        return when (obj) {
            is Byte -> BasicTypes.EncodingType.BYTE
            is Short -> BasicTypes.EncodingType.SHORT
            is Long -> BasicTypes.EncodingType.LONG
            is String -> BasicTypes.EncodingType.STRING
            is Int -> BasicTypes.EncodingType.INT
            is PdxInstance -> {
                if (obj.className == JSONFormatter.JSON_CLASSNAME) {
                    BasicTypes.EncodingType.JSON
                } else {
                    BasicTypes.EncodingType.UNRECOGNIZED
                }
            }
            is ByteArray -> BasicTypes.EncodingType.BINARY
            else -> BasicTypes.EncodingType.UNRECOGNIZED
        }
    }

    @JvmStatic
    fun serializerFromProtoEnum(encodingType: BasicTypes.EncodingType): SerializationType {
        return when (encodingType) {
            BasicTypes.EncodingType.INT -> SerializationType.INT
            BasicTypes.EncodingType.LONG -> SerializationType.LONG
            BasicTypes.EncodingType.SHORT -> SerializationType.SHORT
            BasicTypes.EncodingType.BYTE -> SerializationType.BYTE
            BasicTypes.EncodingType.STRING -> SerializationType.STRING
            BasicTypes.EncodingType.BINARY -> SerializationType.BYTE_BLOB
            BasicTypes.EncodingType.JSON -> SerializationType.JSON
            BasicTypes.EncodingType.FLOAT, BasicTypes.EncodingType.BOOLEAN, BasicTypes.EncodingType.DOUBLE -> TODO()
            else -> TODO("Unknown EncodingType to SerializationType conversion for $encodingType")
        }
    }
}

