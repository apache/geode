/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TYPEDEFS_HPP_
#define __TYPEDEFS_HPP_

#ifdef _LINUX
_Pragma("GCC system_header")
#endif

#include <vector>
#include <string>
#include <algorithm>

    namespace gemfire {
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
      retCode = (OperationCode)(it - allOpCodes.begin());
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

  };  // security
  };  // testframework
};    // gemfire

#endif /*__TYPEDEFS_HPP_*/
