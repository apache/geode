#pragma once

#ifndef APACHE_GEODE_GUARD_051947d883001c8686df187fe716cbd0
#define APACHE_GEODE_GUARD_051947d883001c8686df187fe716cbd0


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

#include <openssl/dh.h>
#include <openssl/asn1t.h>
#include <openssl/x509.h>
#include <string>
#include <vector>
#include <string.h>

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
CPPCACHE_EXPORT int gf_initDhKeys(void** dhCtx, const char* dhAlgo,
                                  const char* ksPath);
CPPCACHE_EXPORT void gf_clearDhKeys(void* dhCtx);
CPPCACHE_EXPORT unsigned char* gf_getPublicKey(void* dhCtx, int* len);
CPPCACHE_EXPORT void gf_setPublicKeyOther(void* dhCtx,
                                          const unsigned char* pubkey,
                                          int length);
CPPCACHE_EXPORT void gf_computeSharedSecret(void* dhCtx);
CPPCACHE_EXPORT unsigned char* gf_encryptDH(void* dhCtx,
                                            const unsigned char* cleartext,
                                            int len, int* retLen);
CPPCACHE_EXPORT unsigned char* gf_decryptDH(void* dhCtx,
                                            const unsigned char* cleartext,
                                            int len, int* retLen);
CPPCACHE_EXPORT bool gf_verifyDH(void* dhCtx, const char* subject,
                                 const unsigned char* challenge,
                                 int challengeLen,
                                 const unsigned char* response, int responseLen,
                                 int* reason);
}

class DHImpl {
 public:
  DH* m_dh;
  std::string m_skAlgo;
  int m_keySize;
  BIGNUM* m_pubKeyOther;
  unsigned char m_key[128];
  std::vector<X509*> m_serverCerts;

  const EVP_CIPHER* getCipherFunc();
  int setSkAlgo(const char* skalgo);

  DHImpl() : m_dh(NULL), m_keySize(0), m_pubKeyOther(NULL) {
    /* adongre
     * CID 28924: Uninitialized scalar field (UNINIT_CTOR)
     */
    memset(m_key, 0, sizeof(m_key));
  }
  static bool m_init;
};

bool DHImpl::m_init = false;


#endif // APACHE_GEODE_GUARD_051947d883001c8686df187fe716cbd0
