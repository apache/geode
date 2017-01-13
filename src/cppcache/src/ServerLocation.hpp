/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __SERVER_LOCATION__
#define __SERVER_LOCATION__
#include <gfcpp/Serializable.hpp>
#include <string>
#include <gfcpp/DataInput.hpp>
#include "Utils.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <ace/INET_Addr.h>
#include <gfcpp/CacheableBuiltins.hpp>
namespace gemfire {
class CPPCACHE_EXPORT ServerLocation : public Serializable {
 public:
  ServerLocation(std::string serverName, int port)
      : Serializable(), m_port(port) {
    if (serverName.length() > 0) {
      m_serverName = CacheableString::create(serverName.c_str());
      LOGDEBUG(
          "ServerLocation::ServerLocation(): creating ServerLocation for %s:%d",
          serverName.c_str(), port);
    }
    makeEpString();
  }
  ServerLocation()
      : Serializable(),
        m_serverName(NULLPTR),
        m_port(-1)  // Default constructor for deserialiozation.
  {}

  ServerLocation(std::string name) {
    /*
    name = Utils::convertHostToCanonicalForm(name.c_str());
    */
    int32_t position = (int32_t)name.find_first_of(":");
    std::string serverName;
    serverName = name.substr(0, position);
    m_port = atoi((name.substr(position + 1)).c_str());
    m_serverName = CacheableString::create(serverName.c_str());
    makeEpString();
  }

  std::string getServerName() const {
    if (m_serverName != NULLPTR) {
      return m_serverName->asChar();
    }
    return "";
  }
  void setServername(CacheableStringPtr sn) { m_serverName = sn; }
  int getPort() const { return m_port; }
  void toData(DataOutput& output) const {
    if (m_serverName != NULLPTR) {
      // output.writeObject( m_serverName );
      output.writeNativeString(m_serverName->asChar());  // changed
    }
    output.writeInt(m_port);
  }
  ServerLocation* fromData(DataInput& input) {
    // input.readObject(m_serverName);
    input.readNativeString(m_serverName);
    input.readInt((int32_t*)&m_port);
    makeEpString();
    return this;
  }
  uint32_t objectSize() const {
    if (m_serverName != NULLPTR) {
      return (uint32_t)sizeof(int) +
             (uint32_t)(m_serverName->length()) * (uint32_t)sizeof(char);
    }
    return 0;
  }
  int8_t typeId() const {
    return 0;  // NOt needed infact
  }
  int8_t DSFID() const {
    return (int8_t)GemfireTypeIdsImpl::FixedIDByte;  // Never used
  }

  int32_t classId() const {
    return 0;  // Never used
  }
  ServerLocation& operator=(const ServerLocation& rhs) {
    if (this == &rhs) return *this;
    this->m_serverName = rhs.m_serverName;
    this->m_port = rhs.m_port;
    // makeEpString();
    return *this;
  }
  ServerLocation(const ServerLocation& rhs) {
    this->m_serverName = rhs.m_serverName;
    this->m_port = rhs.m_port;
    // makeEpString();
  }
  void printInfo() {
    LOGDEBUG(" Got Host %s, and port %d", getServerName().c_str(), m_port);
  }

  bool operator<(const ServerLocation rhs) const {
    std::string s1 = m_serverName->asChar();
    std::string s2 = rhs.m_serverName->asChar();
    if (s1 < s2) {
      return true;
    } else if (s1 == s2) {
      return (m_port < rhs.m_port);
    } else {
      return false;
    }
  }

  bool operator==(const ServerLocation& rhs) const {
    /*char server1[256];
    char server2[256];
    size_t len = 0;
     if (m_serverName != NULLPTR && rhs.getServerName( ).c_str() != NULL) {
      ACE_INET_Addr addr1( m_port, m_serverName->asChar() );
      len = strlen(addr1.get_host_addr());
      memcpy(server1, addr1.get_host_addr(), len);
      server1[len] = '\0';

      ACE_INET_Addr addr2( rhs.getPort( ), rhs.getServerName( ).c_str());
      len = strlen(addr2.get_host_addr());
      memcpy(server2, addr2.get_host_addr(), len);
      server2[len] = '\0';
    }*/

    return (!strcmp(m_serverName->asChar(), rhs.getServerName().c_str()) &&
            (m_port == rhs.getPort()));
  }

  inline bool isValid() const {
    if (m_serverName == NULLPTR) return false;
    return m_serverName->length() > 0 && m_port >= 0;
  }

  inline std::string& getEpString() {
    /*if (m_epString.empty() == false) {
      return m_epString;
    }*/
    return m_epString;
  }

  inline int hashcode() {
    int prime = 31;
    int result = 1;
    // result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + m_port;
    return result;
  }

  void makeEpString();

 protected:
  CacheableStringPtr m_serverName;
  int m_port;
  std::string m_epString;
};
}
#endif
