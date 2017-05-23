package org.apache.geode.protocol.client

import org.apache.geode.pdx.JSONFormatter
import org.apache.geode.pdx.PdxInstance
import org.apache.geode.protocol.protobuf.BasicTypes
import org.apache.geode.serialization.SerializationType

fun getEncodingTypeForObjectKT(o: Any?): BasicTypes.EncodingType {
    return when (o) {
        is String -> BasicTypes.EncodingType.STRING
        is Int -> BasicTypes.EncodingType.INT
        is PdxInstance -> {
            if (o.className == JSONFormatter.JSON_CLASSNAME) BasicTypes.EncodingType.JSON else BasicTypes.EncodingType.UNRECOGNIZED
        }
        is ByteArray -> BasicTypes.EncodingType.BINARY
        else -> BasicTypes.EncodingType.UNRECOGNIZED
    }
}

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
        else -> TODO()
    }
}

