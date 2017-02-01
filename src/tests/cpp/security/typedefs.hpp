#pragma once

#ifndef APACHE_GEODE_GUARD_8a35926faf964b72538dc78519ee0e9b
#define APACHE_GEODE_GUARD_8a35926faf964b72538dc78519ee0e9b

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

#ifdef _LINUX
_Pragma("GCC system_header")
#endif

#include <vector>
#include <string>
#include <algorithm>

    namespace apache {
  namespace geode {
  namespace client {
  namespace testframework {
  namespace security {

  typedef enum {
    ID_NONE = 1,
    ID_DUMMY = 2,
    ID_LDAP = 3,
    ID_PKI = 4,
    ID_NOOP = 5,
    ID_DUMMY2 = 6,
    ID_DUMMY3 = 7
  } ID;

  typedef enum {
    READER_ROLE = 1,
    WRITER_ROLE = 2,
    QUERY_ROLE = 3,
    ADMIN_ROLE = 4,
    NO_ROLE = 5
  } ROLES;

  typedef enum {
    OP_GET = 0,
    OP_CREATE = 1,
    OP_UPDATE = 2,
    OP_DESTROY = 3,
    OP_INVALIDATE = 4,
    OP_REGISTER_INTEREST = 5,
    OP_UNREGISTER_INTEREST = 6,
    OP_CONTAINS_KEY = 7,
    OP_KEY_SET = 8,
    OP_QUERY = 9,
    OP_REGISTER_CQ = 10,
    OP_REGION_CLEAR = 11,
    OP_REGION_CREATE = 12,
    OP_REGION_DESTROY = 13,
    OP_GETALL = 14,
    OP_PUTALL = 15,
    OP_EXECUTE_FUNCTION = 16,
    OP_END = 17
  } OperationCode;

  typedef std::vector<std::string> stringList;
  typedef std::vector<std::string> opCodeStrs;
  typedef std::vector<OperationCode> opCodeList;
  typedef std::vector<std::string> PerClientList;
  typedef std::vector<std::string> readerList;
  typedef std::vector<std::string> writerList;
  typedef std::vector<std::string> adminList;
  typedef std::vector<std::string> queryList;

  const opCodeStrs::value_type opCodeStrArr[] = {"get",
                                                 "create",
                                                 "update",
                                                 "destroy",
                                                 "invalidate",
                                                 "register_interest",
                                                 "unregister_interest",
                                                 "contains_key",
                                                 "key_set",
                                                 "query",
                                                 "register_cq",
                                                 "region_clear",
                                                 "region_create",
                                                 "region_destroy",
                                                 "get_all",
                                                 "put_all",
                                                 "execute_function",
                                                 "end"};

  static OperationCode ATTR_UNUSED strToOpCode(std::string& opCodeStr) {
    static opCodeStrs allOpCodes(
        opCodeStrArr,
        opCodeStrArr + sizeof opCodeStrArr / sizeof *opCodeStrArr);
    opCodeStrs::iterator it =
        std::find(allOpCodes.begin(), allOpCodes.end(), opCodeStr);
    OperationCode retCode = OP_END;

    if (it != allOpCodes.end()) {
      retCode = static_cast<OperationCode>(it - allOpCodes.begin());
      if (allOpCodes[retCode] != opCodeStr) {
        retCode = OP_END;
      }
    }
    return retCode;
  }

  /* for future use
  static std::string opCodeToStr(OperationCode op) {
    static opCodeStrs allOpCodes(opCodeStrArr, opCodeStrArr + sizeof
  opCodeStrArr / sizeof *opCodeStrArr);
    return std::string(allOpCodes[ op ]);
  }
  */

  }  // namespace security
  }  // namespace testframework
  }  // namespace client
  }  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_8a35926faf964b72538dc78519ee0e9b
