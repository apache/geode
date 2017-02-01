#pragma once

#ifndef GEODE_DIFFIEHELLMAN_H_
#define GEODE_DIFFIEHELLMAN_H_


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
#include <ace/DLL.h>
#include <ace/OS.h>
#include <string>
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/Properties.hpp>
#include <ace/Recursive_Thread_Mutex.h>

#define DH_ERR_NO_ERROR 0
#define DH_ERR_UNSUPPORTED_ALGO 1
#define DH_ERR_ILLEGAL_KEYSIZE 2
#define DH_ERR_SUBJECT_NOT_FOUND 3
#define DH_ERR_NO_CERTIFICATES 4
#define DH_ERR_INVALID_SIGN 5

const char SecurityClientDhAlgo[] = "security-client-dhalgo";
const char SecurityClientKsPath[] = "security-client-kspath";

namespace apache {
namespace geode {
namespace client {

class DiffieHellman {
  static ACE_Recursive_Thread_Mutex s_mutex;

 public:
  void initDhKeys(const PropertiesPtr& props);
  void clearDhKeys(void);
  CacheableBytesPtr getPublicKey(void);
  void setPublicKeyOther(const CacheableBytesPtr& pubkey);
  void computeSharedSecret(void);
  CacheableBytesPtr encrypt(const CacheableBytesPtr& cleartext);
  CacheableBytesPtr encrypt(const uint8_t* cleartext, int len);
  CacheableBytesPtr decrypt(const CacheableBytesPtr& cleartext);
  CacheableBytesPtr decrypt(const uint8_t* cleartext, int len);
  bool verify(const CacheableStringPtr& subject,
              const CacheableBytesPtr& challenge,
              const CacheableBytesPtr& response);

  static void initOpenSSLFuncPtrs();

  DiffieHellman()
      : /* adongre
         * CID 28933: Uninitialized pointer field (UNINIT_CTOR)
         */
        m_dhCtx((void*)0) {}

 private:
  void* m_dhCtx;
  static bool m_inited;
  static void* getOpenSSLFuncPtr(const char* function_name);

  // OpenSSL Func Ptrs: Declare Func Ptr type and a static variable of FuncPtr
  // type.
  // Convention: <Orig Func Name>_Type and <Orig Func Name>_Ptr
  typedef int (*gf_initDhKeys_Type)(void** dhCtx, const char* dhAlgo,
                                    const char* ksPath);
  typedef void (*gf_clearDhKeys_Type)(void* dhCtx);
  typedef unsigned char* (*gf_getPublicKey_Type)(void* dhCtx, int* len);
  typedef void (*gf_setPublicKeyOther_Type)(void* dhCtx,
                                            const unsigned char* pubkey,
                                            int length);
  typedef void (*gf_computeSharedSecret_Type)(void* dhCtx);
  typedef unsigned char* (*gf_encryptDH_Type)(void* dhCtx,
                                              const unsigned char* cleartext,
                                              int len, int* retLen);
  typedef unsigned char* (*gf_decryptDH_Type)(void* dhCtx,
                                              const unsigned char* cleartext,
                                              int len, int* retLen);
  typedef bool (*gf_verifyDH_Type)(void* dhCtx, const char* subject,
                                   const unsigned char* challenge,
                                   int challengeLen,
                                   const unsigned char* response,
                                   int responseLen, int* reason);

#define DECLARE_DH_FUNC_PTR(OrigName) static OrigName##_Type OrigName##_Ptr;

  DECLARE_DH_FUNC_PTR(gf_initDhKeys)
  DECLARE_DH_FUNC_PTR(gf_clearDhKeys)
  DECLARE_DH_FUNC_PTR(gf_getPublicKey)
  DECLARE_DH_FUNC_PTR(gf_setPublicKeyOther)
  DECLARE_DH_FUNC_PTR(gf_computeSharedSecret)
  DECLARE_DH_FUNC_PTR(gf_encryptDH)
  DECLARE_DH_FUNC_PTR(gf_decryptDH)
  DECLARE_DH_FUNC_PTR(gf_verifyDH)

  static ACE_DLL m_dll;

};  // class DiffieHellman
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_DIFFIEHELLMAN_H_
