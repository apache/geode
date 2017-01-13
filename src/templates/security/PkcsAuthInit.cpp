/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "PkcsAuthInit.hpp"
#include "gfcpp/Properties.hpp"
#include "gfcpp/CacheableBuiltins.hpp"
#include "gfcpp/ExceptionTypes.hpp"
#include "stdio.h"

namespace gemfire {

extern "C" {
LIBEXP AuthInitialize* createPKCSAuthInitInstance() {
  return new PKCSAuthInit();
}

uint8_t* createSignature(EVP_PKEY* key, X509* cert,
                         const unsigned char* inputBuffer,
                         uint32_t inputBufferLen, unsigned int* signatureLen) {
  if (key == NULL || cert == NULL || inputBuffer == NULL) {
    return NULL;
  }

  const EVP_MD* signatureDigest = EVP_get_digestbyobj(cert->sig_alg->algorithm);
  EVP_MD_CTX signatureCtx;
  EVP_MD_CTX_init(&signatureCtx);
  uint8_t* signatureData = new uint8_t[EVP_PKEY_size(key)];

  bool result =
      (EVP_SignInit_ex(&signatureCtx, signatureDigest, NULL) &&
       EVP_SignUpdate(&signatureCtx, inputBuffer, inputBufferLen) &&
       EVP_SignFinal(&signatureCtx, signatureData, signatureLen, key));

  EVP_MD_CTX_cleanup(&signatureCtx);
  if (result) {
    return signatureData;
  }
  return NULL;
}

bool readPKCSPublicPrivateKey(FILE* keyStoreFP, const char* keyStorePassword,
                              EVP_PKEY** outPrivateKey, X509** outCertificate) {
  PKCS12* p12;

  if ((keyStoreFP == NULL) || (keyStorePassword == NULL) ||
      (keyStorePassword[0] == '\0')) {
    return (false);
  }

  p12 = d2i_PKCS12_fp(keyStoreFP, NULL);

  if (p12 == NULL) {
    return (false);
  }

  if (!PKCS12_parse(p12, keyStorePassword, outPrivateKey, outCertificate,
                    NULL)) {
    return (false);
  }

  PKCS12_free(p12);

  return (outPrivateKey && outCertificate);
}

bool openSSLInit() {
  OpenSSL_add_all_algorithms();
  ERR_load_crypto_strings();

  return true;
}

static bool s_initDone = openSSLInit();
}
// end of extern "C"

PropertiesPtr PKCSAuthInit::getCredentials(PropertiesPtr& securityprops,
                                           const char* server) {
  if (!s_initDone) {
    throw AuthenticationFailedException(
        "PKCSAuthInit::getCredentials: "
        "OpenSSL initialization failed.");
  }
  if (securityprops == NULLPTR || securityprops->getSize() <= 0) {
    throw AuthenticationRequiredException(
        "PKCSAuthInit::getCredentials: "
        "No security-* properties are set.");
  }

  CacheableStringPtr keyStoreptr = securityprops->find(KEYSTORE_FILE_PATH);

  const char* keyStorePath = keyStoreptr->asChar();

  if (keyStorePath == NULL) {
    throw AuthenticationFailedException(
        "PKCSAuthInit::getCredentials: "
        "key-store file path property KEYSTORE_FILE_PATH not set.");
  }

  CacheableStringPtr aliasptr = securityprops->find(KEYSTORE_ALIAS);

  const char* alias = aliasptr->asChar();

  if (alias == NULL) {
    throw AuthenticationFailedException(
        "PKCSAuthInit::getCredentials: "
        "key-store alias property KEYSTORE_ALIAS not set.");
  }

  CacheableStringPtr keyStorePassptr = securityprops->find(KEYSTORE_PASSWORD);

  const char* keyStorePass = keyStorePassptr->asChar();

  if (keyStorePass == NULL) {
    throw AuthenticationFailedException(
        "PKCSAuthInit::getCredentials: "
        "key-store password property KEYSTORE_PASSWORD not set.");
  }
  DataOutput additionalMsg;

  FILE* keyStoreFP = fopen(keyStorePath, "r");
  if (keyStoreFP == NULL) {
    char msg[1024];
    sprintf(msg, "PKCSAuthInit::getCredentials: Unable to open keystore %s",
            keyStorePath);
    throw AuthenticationFailedException(msg);
  }

  EVP_PKEY* privateKey = NULL;
  X509* cert = NULL;

  /* Read the Public and Private Key from keystore in file */
  if (!readPKCSPublicPrivateKey(keyStoreFP, keyStorePass, &privateKey, &cert)) {
    fclose(keyStoreFP);
    char msg[1024];
    sprintf(msg,
            "PKCSAuthInit::getCredentials: Unable to read PKCS "
            "public key from %s",
            keyStorePath);
    throw AuthenticationFailedException(msg);
  }

  fclose(keyStoreFP);

  additionalMsg.writeUTF(alias);

  uint32_t dataLen;
  char* data = (char*)additionalMsg.getBuffer(&dataLen);
  unsigned int lengthEncryptedData = 0;

  // Skip first two bytes of the java UTF-8 encoded string
  // containing the length of the string.
  uint8_t* signatureData = createSignature(
      privateKey, cert, reinterpret_cast<unsigned char*>(data + 2), dataLen - 2,
      &lengthEncryptedData);
  EVP_PKEY_free(privateKey);
  X509_free(cert);
  if (signatureData == NULL) {
    throw AuthenticationFailedException(
        "PKCSAuthInit::getCredentials: "
        "Unable to create signature");
  }
  CacheablePtr signatureValPtr =
      CacheableBytes::createNoCopy(signatureData, lengthEncryptedData);

  PropertiesPtr credentials = Properties::create();
  credentials->insert(KEYSTORE_ALIAS, alias);
  credentials->insert(CacheableString::create(SIGNATURE_DATA), signatureValPtr);
  return credentials;
}
}  // namespace gemfire
