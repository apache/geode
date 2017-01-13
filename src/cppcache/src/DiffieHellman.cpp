/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "DiffieHellman.hpp"
#include <gfcpp/Log.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <ace/Guard_T.h>
namespace gemfire {

ACE_DLL DiffieHellman::m_dll;
bool DiffieHellman::m_inited = false;
ACE_Recursive_Thread_Mutex DiffieHellman::s_mutex;

#define INIT_DH_FUNC_PTR(OrigName) \
  DiffieHellman::OrigName##_Type DiffieHellman::OrigName##_Ptr = NULL;

INIT_DH_FUNC_PTR(gf_initDhKeys)
INIT_DH_FUNC_PTR(gf_clearDhKeys)
INIT_DH_FUNC_PTR(gf_getPublicKey)
INIT_DH_FUNC_PTR(gf_setPublicKeyOther)
INIT_DH_FUNC_PTR(gf_computeSharedSecret)
INIT_DH_FUNC_PTR(gf_encryptDH)
INIT_DH_FUNC_PTR(gf_decryptDH)
INIT_DH_FUNC_PTR(gf_verifyDH)

void* DiffieHellman::getOpenSSLFuncPtr(const char* function_name) {
  void* func = m_dll.symbol(function_name);
  if (func == NULL) {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "cannot find function %s in library %s",
                     function_name, "cryptoImpl");
    LOGERROR(msg);
    throw IllegalStateException(msg);
  }
  return func;
}

void DiffieHellman::initOpenSSLFuncPtrs() {
  if (DiffieHellman::m_inited) {
    return;
  }

  const char* libName = "cryptoImpl";

  if (m_dll.open(libName, ACE_DEFAULT_SHLIB_MODE, 0) == -1) {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "cannot open library: %s", libName);
    LOGERROR(msg);
    throw FileNotFoundException(msg);
  }

#define ASSIGN_DH_FUNC_PTR(OrigName) \
  OrigName##_Ptr = (OrigName##_Type)getOpenSSLFuncPtr(#OrigName);

  ASSIGN_DH_FUNC_PTR(gf_initDhKeys)
  ASSIGN_DH_FUNC_PTR(gf_clearDhKeys)
  ASSIGN_DH_FUNC_PTR(gf_getPublicKey)
  ASSIGN_DH_FUNC_PTR(gf_setPublicKeyOther)
  ASSIGN_DH_FUNC_PTR(gf_computeSharedSecret)
  ASSIGN_DH_FUNC_PTR(gf_encryptDH)
  ASSIGN_DH_FUNC_PTR(gf_decryptDH)
  ASSIGN_DH_FUNC_PTR(gf_verifyDH)

  DiffieHellman::m_inited = true;
}

void DiffieHellman::initDhKeys(const PropertiesPtr& props) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(DiffieHellman::s_mutex);
  m_dhCtx = NULL;

  CacheableStringPtr dhAlgo = props->find(SecurityClientDhAlgo);
  CacheableStringPtr ksPath = props->find(SecurityClientKsPath);

  // Null check only for DH Algo
  if (dhAlgo == NULLPTR) {
    LOGFINE("DH algo not available");
    return;
  }

  int error = gf_initDhKeys_Ptr(&m_dhCtx, dhAlgo->asChar(),
                                ksPath != NULLPTR ? ksPath->asChar() : NULL);

  if (error == DH_ERR_UNSUPPORTED_ALGO) {  // Unsupported Algorithm
    char msg[64] = {'\0'};
    ACE_OS::snprintf(msg, 64, "Algorithm %s is not supported.",
                     dhAlgo->asChar());
    throw IllegalArgumentException(msg);
  } else if (error == DH_ERR_ILLEGAL_KEYSIZE) {  // Illegal Key size
    char msg[64] = {'\0'};
    ACE_OS::snprintf(msg, 64, "Illegal key size for algorithm %s.",
                     dhAlgo->asChar());
    throw IllegalArgumentException(msg);
  } else if (m_dhCtx == NULL) {
    throw IllegalStateException(
        "Could not initialize the Diffie-Hellman helper");
  }
}

void DiffieHellman::clearDhKeys(void) {
  // Sanity check for accidental calls
  if (gf_clearDhKeys_Ptr == NULL) {
    return;
  }

  gf_clearDhKeys_Ptr(m_dhCtx);

  m_dhCtx = NULL;

  /*
  //reset all pointers
#define CLEAR_DH_FUNC_PTR(OrigName) \
  OrigName##_Ptr = NULL;

  CLEAR_DH_FUNC_PTR(gf_initDhKeys)
  CLEAR_DH_FUNC_PTR(gf_clearDhKeys)
  CLEAR_DH_FUNC_PTR(gf_getPublicKey)
  CLEAR_DH_FUNC_PTR(gf_setPublicKeyOther)
  CLEAR_DH_FUNC_PTR(gf_computeSharedSecret)
  CLEAR_DH_FUNC_PTR(gf_encryptDH)
  CLEAR_DH_FUNC_PTR(gf_verifyDH)
  */

  return;
}

CacheableBytesPtr DiffieHellman::getPublicKey(void) {
  int keyLen = 0;
  unsigned char* pubKeyPtr = gf_getPublicKey_Ptr(m_dhCtx, &keyLen);
  return CacheableBytes::createNoCopy(pubKeyPtr, keyLen);
}

void DiffieHellman::setPublicKeyOther(const CacheableBytesPtr& pubkey) {
  return gf_setPublicKeyOther_Ptr(m_dhCtx, pubkey->value(), pubkey->length());
}

void DiffieHellman::computeSharedSecret(void) {
  return gf_computeSharedSecret_Ptr(m_dhCtx);
}

CacheableBytesPtr DiffieHellman::encrypt(const CacheableBytesPtr& cleartext) {
  return encrypt(cleartext->value(), cleartext->length());
}

CacheableBytesPtr DiffieHellman::encrypt(const uint8_t* cleartext, int len) {
  int cipherLen = 0;
  unsigned char* ciphertextPtr =
      gf_encryptDH_Ptr(m_dhCtx, cleartext, len, &cipherLen);
  return CacheableBytes::createNoCopy(ciphertextPtr, cipherLen);
}

CacheableBytesPtr DiffieHellman::decrypt(const CacheableBytesPtr& cleartext) {
  return decrypt(cleartext->value(), cleartext->length());
}

CacheableBytesPtr DiffieHellman::decrypt(const uint8_t* cleartext, int len) {
  int cipherLen = 0;
  unsigned char* ciphertextPtr =
      gf_decryptDH_Ptr(m_dhCtx, cleartext, len, &cipherLen);
  return CacheableBytes::createNoCopy(ciphertextPtr, cipherLen);
}

bool DiffieHellman::verify(const CacheableStringPtr& subject,
                           const CacheableBytesPtr& challenge,
                           const CacheableBytesPtr& response) {
  int errCode = DH_ERR_NO_ERROR;
  LOGDEBUG("DiffieHellman::verify");
  bool result = gf_verifyDH_Ptr(m_dhCtx, subject->asChar(), challenge->value(),
                                challenge->length(), response->value(),
                                response->length(), &errCode);
  LOGDEBUG("DiffieHellman::verify 2");
  if (errCode == DH_ERR_SUBJECT_NOT_FOUND) {
    LOGERROR("Subject name %s not found in imported certificates.",
             subject->asChar());
  } else if (errCode == DH_ERR_NO_CERTIFICATES) {
    LOGERROR("No imported certificates.");
  } else if (errCode == DH_ERR_INVALID_SIGN) {
    LOGERROR("Signature varification failed.");
  }

  return result;
}

};  // namespace gemfire
