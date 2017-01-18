#ifndef __GEMFIRE_USERATTRIBUTES_H__
#define __GEMFIRE_USERATTRIBUTES_H__

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Properties.hpp>
#include "TcrEndpoint.hpp"
#include <ace/TSS_T.h>
#include <string>
#include <map>

namespace gemfire {
class ProxyCache;
typedef SharedPtr<ProxyCache> ProxyCachePtr;
class ThinClientPoolDM;
class UserConnectionAttributes {
 public:
  UserConnectionAttributes(TcrEndpoint* endpoint, uint64_t id) {
    m_numberOfTimesEndpointFailed = endpoint->numberOfTimesFailed();
    m_connectedEndpoint = endpoint;
    m_uniqueId = id;
    m_isAuthenticated = true;
  }

  ~UserConnectionAttributes() {}

  TcrEndpoint* getEndpoint() { return m_connectedEndpoint; }

  void setEndpoint(TcrEndpoint* endpoint) { m_connectedEndpoint = endpoint; }

  int64_t getUniqueId() { return m_uniqueId; }

  void setUniqueId(int64_t id) { m_uniqueId = id; }

  void setUnAuthenticated() { m_isAuthenticated = false; }

  bool isAuthenticated() {
    // second condition checks whether endpoint got failed and again up
    return m_isAuthenticated && (m_connectedEndpoint->numberOfTimesFailed() ==
                                 m_numberOfTimesEndpointFailed);
  }

 private:
  TcrEndpoint* m_connectedEndpoint;
  int64_t m_uniqueId;
  bool m_isAuthenticated;
  int32_t m_numberOfTimesEndpointFailed;
  // UserConnectionAttributes(const UserConnectionAttributes &);
  // UserConnectionAttributes & operator =(const UserConnectionAttributes &);
};

class CPPCACHE_EXPORT UserAttributes : public SharedBase {
  // TODO: need to add lock here so that user should not be authenticated at two
  // servers
 public:
  ~UserAttributes();
  UserAttributes(PropertiesPtr credentials, PoolPtr pool,
                 ProxyCache* proxyCache);

  bool isCacheClosed();

  ProxyCachePtr getProxyCache();

  PoolPtr getPool();

  void setConnectionAttributes(TcrEndpoint* endpoint, uint64_t id) {
    m_isUserAuthenticated = true;
    UserConnectionAttributes* ucb = new UserConnectionAttributes(endpoint, id);
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_listLock);
    // m_connectionAttr.push_back(ucb);
    std::string fullName(endpoint->name().c_str());
    m_connectionAttr[fullName] = ucb;
  }

  void unAuthenticateEP(TcrEndpoint* endpoint);

  UserConnectionAttributes* getConnectionAttribute();
  UserConnectionAttributes* getConnectionAttribute(TcrEndpoint* ep);
  PropertiesPtr getCredentials();

  std::map<std::string, UserConnectionAttributes*>& getUserConnectionServers() {
    return m_connectionAttr;
  }

  void unSetCredentials() { m_credentials = NULLPTR; }

  bool isEndpointAuthenticated(TcrEndpoint* ep);

 private:
  std::map<std::string, UserConnectionAttributes*> m_connectionAttr;
  PropertiesPtr m_credentials;
  // ThinClientPoolDM m_pool;
  ACE_Recursive_Thread_Mutex m_listLock;
  bool m_isUserAuthenticated;
  ProxyCachePtr m_proxyCache;
  PoolPtr m_pool;

  // Disallow copy constructor and assignment operator.
  UserAttributes(const UserAttributes&);
  UserAttributes& operator=(const UserAttributes&);
};

typedef SharedPtr<UserAttributes> UserAttributesPtr;

class TSSUserAttributesWrapper {
 private:
  UserAttributesPtr m_userAttribute;
  TSSUserAttributesWrapper& operator=(const TSSUserAttributesWrapper&);
  TSSUserAttributesWrapper(const TSSUserAttributesWrapper&);

 public:
  static ACE_TSS<TSSUserAttributesWrapper> s_gemfireTSSUserAttributes;
  UserAttributesPtr getUserAttributes() { return m_userAttribute; }
  void setUserAttributes(UserAttributesPtr userAttr) {
    m_userAttribute = userAttr;
  }
  TSSUserAttributesWrapper() : m_userAttribute(NULLPTR) {}
  ~TSSUserAttributesWrapper() {}
};

class GuardUserAttribures {
 public:
  GuardUserAttribures();

  GuardUserAttribures(ProxyCachePtr proxyCache);

  void setProxyCache(ProxyCachePtr proxyCache);

  ~GuardUserAttribures();

 private:
  ProxyCachePtr m_proxyCache;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_USERATTRIBUTES_H__
