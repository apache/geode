#ifndef __DIFFIEHELLMAN__
#define __DIFFIEHELLMAN__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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

namespace gemfire {

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

};  // namespace gemfire

#endif  // __DIFFIEHELLMAN__
