package io.pivotal.gemfire.spark.connector

import com.esotericsoftware.kryo.Kryo
import io.pivotal.gemfire.spark.connector.internal.oql.UndefinedSerializer
import org.apache.spark.serializer.KryoRegistrator
import com.gemstone.gemfire.cache.query.internal.Undefined

class GemFireKryoRegistrator extends KryoRegistrator{

  override def registerClasses(kyro: Kryo): Unit = {
    kyro.addDefaultSerializer(classOf[Undefined], classOf[UndefinedSerializer])
  }
}
