#include "DataInput.hpp"

#include "impl/SerializationRegistry.hpp"

namespace gemfire {

  void DataInput::readObjectInternal( SerializablePtr& ptr, int8_t typeId )
  {
    ptr = SerializationRegistry::deserialize( *this, typeId );
  }
}
