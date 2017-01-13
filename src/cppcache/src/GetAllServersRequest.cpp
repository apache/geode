#include "GetAllServersRequest.hpp"

using namespace gemfire;

void GetAllServersRequest::toData(DataOutput& output) const {
  output.writeObject(m_serverGroup);
}

Serializable* GetAllServersRequest::fromData(DataInput& input) {
  input.readObject(m_serverGroup);
  return this;
}
