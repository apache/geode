#ifndef _DHEIMPL_HPP_INCLUDED_
#define _DHEIMPL_HPP_INCLUDED_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <openssl/dh.h>
#include <openssl/asn1t.h>
#include <openssl/x509.h>
#include <string>
#include <vector>

#include <gfcpp/gf_base.hpp>

#define DH_ERR_NO_ERROR 0
#define DH_ERR_UNSUPPORTED_ALGO 1
#define DH_ERR_ILLEGAL_KEYSIZE 2
#define DH_ERR_SUBJECT_NOT_FOUND 3
#define DH_ERR_NO_CERTIFICATES 4
#define DH_ERR_INVALID_SIGN 5

#ifdef _DEBUG
#define LOGDH printf
#else
#define LOGDH(...)
#endif

//  We need to declare our own structures and macros for
// DH public key x509 encoding because it's not available in
// OpenSSL yet.
typedef struct DH_pubkey_st {
  X509_ALGOR* algor;
  ASN1_BIT_STRING* public_key;
  EVP_PKEY* pkey;
} DH_PUBKEY;

extern "C" {
CPPCACHE_EXPORT int gf_initDhKeys(const char* dhAlgo, const char* ksPath);
CPPCACHE_EXPORT void gf_clearDhKeys(void);
CPPCACHE_EXPORT unsigned char* gf_getPublicKey(int* len);
CPPCACHE_EXPORT void gf_setPublicKeyOther(const unsigned char* pubkey,
                                          int length);
CPPCACHE_EXPORT void gf_computeSharedSecret(void);
CPPCACHE_EXPORT unsigned char* gf_encryptDH(const unsigned char* cleartext,
                                            int len, int* retLen);
CPPCACHE_EXPORT bool gf_verifyDH(const char* subject,
                                 const unsigned char* challenge,
                                 int challengeLen,
                                 const unsigned char* response, int responseLen,
                                 int* reason);
}

#endif  // _DHEIMPL_HPP_INCLUDED_
