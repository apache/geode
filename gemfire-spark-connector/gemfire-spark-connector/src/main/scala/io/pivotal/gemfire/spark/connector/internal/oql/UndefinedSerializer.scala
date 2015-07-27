package io.pivotal.gemfire.spark.connector.internal.oql

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Output, Input}
import com.gemstone.gemfire.cache.query.QueryService
import com.gemstone.gemfire.cache.query.internal.Undefined

/**
 * This is the customized serializer to serialize QueryService.UNDEFINED,
 * i.e. com.gemstone.gemfire.cache.query.internal.Undefined, in order to
 * guarantee the singleton Undefined after its deserialization within Spark.
 */
class UndefinedSerializer extends Serializer[Undefined] {

  def write(kryo: Kryo, output: Output, u: Undefined) {
    //Only serialize a byte for Undefined
    output.writeByte(u.getDSFID)
  }

  def read (kryo: Kryo, input: Input, tpe: Class[Undefined]): Undefined = {
    //Read DSFID of Undefined
    input.readByte()
    QueryService.UNDEFINED match {
      case null => new Undefined
      case _ =>
        //Avoid calling Undefined constructor again.
        QueryService.UNDEFINED.asInstanceOf[Undefined]
    }
  }
}
