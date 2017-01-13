/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "DHImpl.hpp"

#include <openssl/objects.h>
#include <openssl/stack.h>
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/asn1.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/rsa.h>
#include <cstring>
#include <cctype>

#include <gfcpp/gfcpp_globals.hpp>

/*
static DH * m_dh = NULL;
static string m_skAlgo;
static int    m_keySize = 0;
static BIGNUM * m_pubKeyOther = NULL;
static unsigned char m_key[128] = {0};
static std::vector<X509*> m_serverCerts;
*/

static const char *dhP =
    "13528702063991073999718992897071702177131142188276542919088770094024269"
    "73079899070080419278066109785292538223079165925365098181867673946"
    "34756714063947534092593553024224277712367371302394452615862654308"
    "11180902979719649450105660478776364198726078338308557022096810447"
    "3500348898008043285865193451061481841186553";

static const char *dhG =
    "13058345680719715096166513407513969537624553636623932169016704425008150"
    "56576152779768716554354314319087014857769741104157332735258102835"
    "93126577393912282416840649805564834470583437473176415335737232689"
    "81480201869671811010996732593655666464627559582258861254878896534"
    "1273697569202082715873518528062345259949959";

static const int dhL = 1023;

static int DH_PUBKEY_set(DH_PUBKEY **x, EVP_PKEY *pkey);
static EVP_PKEY *DH_PUBKEY_get(DH_PUBKEY *key);
/*
static const EVP_CIPHER* getCipherFunc();
static int setSkAlgo(const char * skalgo);
*/

ASN1_SEQUENCE(
    DH_PUBKEY) = {ASN1_SIMPLE(DH_PUBKEY, algor, X509_ALGOR),
                  ASN1_SIMPLE(DH_PUBKEY, public_key,
                              ASN1_BIT_STRING)} ASN1_SEQUENCE_END(DH_PUBKEY)

    // This gives us the i2d/d2i x.509 (ASN1 DER) encode/decode functions
    IMPLEMENT_ASN1_FUNCTIONS(DH_PUBKEY)

    // Returns Error code
    int gf_initDhKeys(void **dhCtx, const char *dhAlgo, const char *ksPath) {
  int errorCode = DH_ERR_NO_ERROR;  // No error;

  DHImpl *dhimpl = new DHImpl();
  *dhCtx = (void *)dhimpl;

  // ksPath can be null
  if (dhimpl->m_dh != NULL || dhAlgo == NULL || strlen(dhAlgo) == 0) {
    return errorCode;
  }

  // set the symmetric cipher algorithm name
  errorCode = dhimpl->setSkAlgo(dhAlgo);
  if (errorCode != DH_ERR_NO_ERROR) {
    return errorCode;
  }

  // do add-all here or outside in DS::connect?
  if (!DHImpl::m_init) {
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
    DHImpl::m_init = true;
  }

  dhimpl->m_dh = DH_new();

  LOGDH(" DHInit: P ptr is %p", dhimpl->m_dh->p);
  LOGDH(" DHInit: G ptr is %p", dhimpl->m_dh->g);
  LOGDH(" DHInit: length is %d", dhimpl->m_dh->length);

  int ret = -1;

  ret = BN_dec2bn(&dhimpl->m_dh->p, dhP);
  LOGDH(" DHInit: BN_dec2bn dhP ret %d", ret);

  ret = BN_dec2bn(&dhimpl->m_dh->g, dhG);
  LOGDH(" DHInit: BN_dec2bn dhG ret %d", ret);

  dhimpl->m_dh->length = dhL;

  ret = DH_generate_key(dhimpl->m_dh);
  LOGDH(" DHInit: DH_generate_key ret %d", ret);

  ret = BN_num_bits(dhimpl->m_dh->priv_key);
  LOGDH(" DHInit: BN_num_bits priv_key is %d", ret);

  ret = BN_num_bits(dhimpl->m_dh->pub_key);
  LOGDH(" DHInit: BN_num_bits pub_key is %d", ret);

  int codes = 0;
  ret = DH_check(dhimpl->m_dh, &codes);
  LOGDH(" DHInit: DH_check ret %d : codes is 0x%04X", ret, codes);
  LOGDH(" DHInit: DH_size is %d", DH_size(dhimpl->m_dh));

  // load the server's RSA public key for server authentication
  // note that OpenSSL 0.9.8g has a bug where it can read only the first one in
  // the keystore

  LOGDH(" Loading keystore...");

  if (ksPath == NULL || strlen(ksPath) == 0) {
    LOGDH("Property \"security-client-kspath\" 's value is NULL.");
    return errorCode;
  }
  FILE *keyStoreFP = NULL;
  keyStoreFP = fopen(ksPath, "r");

  LOGDH(" kspath is [%s]", ksPath);
  LOGDH(" keystore FILE ptr is %p", keyStoreFP);

  // Read from pem file and put into.
  X509 *cert = NULL;
  do {
    cert = PEM_read_X509(keyStoreFP, NULL, NULL, NULL);

    if (cert != NULL) {
      char *certSubject = NULL;
      X509_NAME *xname = X509_get_subject_name(cert);
      certSubject = X509_NAME_oneline(xname, NULL, 0);
      LOGDH(" Imported cert with subject : [%s]\n", certSubject);
      dhimpl->m_serverCerts.push_back(cert);
    }
  } while (cert != NULL);

  LOGDH(" Total certificats imported # %d", dhimpl->m_serverCerts.size());

  fclose(keyStoreFP);

  return errorCode;
}

void gf_clearDhKeys(void *dhCtx) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  if (dhimpl->m_dh != NULL) {
    DH_free(dhimpl->m_dh);
    dhimpl->m_dh = NULL;
  }

  std::vector<X509 *>::const_iterator iter;
  for (iter = dhimpl->m_serverCerts.begin();
       iter != dhimpl->m_serverCerts.end(); ++iter) {
    X509_free(*iter);
  }

  dhimpl->m_serverCerts.clear();

  if (dhimpl->m_pubKeyOther != NULL) {
    BN_free(dhimpl->m_pubKeyOther);
    dhimpl->m_pubKeyOther = NULL;
  }

  memset(dhimpl->m_key, 0, 128);

  // EVP_cleanup();
}

unsigned char *gf_getPublicKey(void *dhCtx, int *pLen) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  if (dhimpl->m_dh->pub_key == NULL || pLen == NULL) {
    return NULL;
  }

  int numBytes = BN_num_bytes(dhimpl->m_dh->pub_key);

  if (numBytes <= 0) {
    return NULL;
  }

  EVP_PKEY *evppubkey = EVP_PKEY_new();
  LOGDH(" before assign DH ptr is %p\n", dhimpl->m_dh);
  int ret = EVP_PKEY_assign_DH(evppubkey, dhimpl->m_dh);
  LOGDH(" evp assign ret %d\n", ret);
  LOGDH(" after assign DH ptr is %p\n", dhimpl->m_dh);
  DH_PUBKEY *dhpubkey = NULL;
  ret = DH_PUBKEY_set(&dhpubkey, evppubkey);
  LOGDH(" DHPUBKEYset ret %d\n", ret);
  int len = i2d_DH_PUBKEY(dhpubkey, NULL);
  unsigned char *pubkey = new unsigned char[len];
  unsigned char *temp = pubkey;
  //
  //  Note, this temp pointer is needed because OpenSSL increments the pointer
  //  passed in
  // so that following encoding can be done at the current output location, this
  // will cause a
  // problem if we try to free the pointer which has been moved by OpenSSL.
  //
  i2d_DH_PUBKEY(dhpubkey, &temp);

  //  TODO: uncomment this - causing problem in computeSecret?
  // DH_PUBKEY_free(dhpubkey);
  // EVP_PKEY_free(evppubkey);

  LOGDH(" after evp free DH ptr is %p\n", dhimpl->m_dh);
  *pLen = len;
  return pubkey;
}

void gf_setPublicKeyOther(void *dhCtx, const unsigned char *pubkey,
                          int length) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  if (dhimpl->m_pubKeyOther != NULL) {
    BN_free(dhimpl->m_pubKeyOther);
    dhimpl->m_pubKeyOther = NULL;
  }

  const unsigned char *temp = pubkey;
  DH_PUBKEY *dhpubkey = d2i_DH_PUBKEY(NULL, &temp, length);
  LOGDH(" setPubKeyOther: after d2i_dhpubkey ptr is %p\n", dhpubkey);
  EVP_PKEY *evppkey = DH_PUBKEY_get(dhpubkey);
  LOGDH(" setPubKeyOther: after dhpubkey get evp ptr is %p\n", evppkey);
  LOGDH(" setPubKeyOther: before BNdup ptr is %p\n", dhimpl->m_pubKeyOther);
  dhimpl->m_pubKeyOther = BN_dup(evppkey->pkey.dh->pub_key);
  LOGDH(" setPubKeyOther: after BNdup ptr is %p\n", dhimpl->m_pubKeyOther);
  EVP_PKEY_free(evppkey);
  DH_PUBKEY_free(dhpubkey);

  int codes = 0;
  int ret ATTR_UNUSED =
      DH_check_pub_key(dhimpl->m_dh, dhimpl->m_pubKeyOther, &codes);
  LOGDH(" DHInit: DH_check_pub_key ret %d\n", ret);
  LOGDH(" DHInit: DH check_pub_key codes is 0x%04X\n", codes);
}

void gf_computeSharedSecret(void *dhCtx) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  LOGDH("COMPUTE: DH ptr %p, pubkeyOther ptr %p", dhimpl->m_dh,
        dhimpl->m_pubKeyOther);

  LOGDH("DHcomputeKey DHSize is %d", DH_size(dhimpl->m_dh));
  int ret ATTR_UNUSED =
      DH_compute_key(dhimpl->m_key, dhimpl->m_pubKeyOther, dhimpl->m_dh);
  LOGDH("DHcomputeKey ret %d : Compute err(%d): %s", ret, ERR_get_error(),
        ERR_error_string(ERR_get_error(), NULL));
}

int DHImpl::setSkAlgo(const char *skalgo) {
  int errCode = DH_ERR_NO_ERROR;

  std::string inAlgo(skalgo);
  size_t colIdx = inAlgo.find(':');
  std::string algoStr =
      (colIdx == std::string::npos) ? inAlgo : inAlgo.substr(0, colIdx);
  int keySize = 0;

  // Convert input algo to lower case to support case insensitivity
  for (unsigned int i = 0; i < algoStr.size(); i++) {
    algoStr[i] = tolower(algoStr[i]);
  }

  if (algoStr == "aes") {
    keySize = (colIdx == std::string::npos)
                  ? 128
                  : atoi(inAlgo.substr(colIdx + 1).c_str());
    if (keySize == 128 || keySize == 192 || keySize == 256) {
      m_skAlgo = "AES";
      m_keySize = keySize;
    } else {
      return DH_ERR_ILLEGAL_KEYSIZE;
    }
  } else if (algoStr == "blowfish") {
    keySize = (colIdx == std::string::npos)
                  ? 128
                  : atoi(inAlgo.substr(colIdx + 1).c_str());
    if (keySize >= 128 && keySize <= 448) {
      m_skAlgo = "Blowfish";
      m_keySize = keySize;
    } else {
      return DH_ERR_ILLEGAL_KEYSIZE;
    }
  } else if (algoStr == "desede") {  // No keysize should be given
    if (colIdx == std::string::npos) {
      m_skAlgo = "DESede";
      m_keySize = 192;
    } else {
      return DH_ERR_ILLEGAL_KEYSIZE;
    }
  } else {
    return DH_ERR_UNSUPPORTED_ALGO;
  }

  LOGDH(" DH: Got SK algo as %s", m_skAlgo.c_str());
  LOGDH(" DH: Got keySize as %d", m_keySize);

  return errCode;
}

const EVP_CIPHER *DHImpl::getCipherFunc() {
  if (m_skAlgo == "AES") {
    if (m_keySize == 192) {
      return EVP_aes_192_cbc();
    } else if (m_keySize == 256) {
      return EVP_aes_256_cbc();
    } else {  // Default
      return EVP_aes_128_cbc();
    }
  } else if (m_skAlgo == "Blowfish") {
    return EVP_bf_cbc();
  } else if (m_skAlgo == "DESede") {
    return EVP_des_ede3_cbc();
  } else {
    LOGDH("ERROR: Unsupported DH Algorithm");
    return NULL;
  }
}

unsigned char *gf_encryptDH(void *dhCtx, const unsigned char *cleartext,
                            int len, int *retLen) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  // Validation
  if (cleartext == NULL || len < 1 || retLen == NULL) {
    return NULL;
  }

  LOGDH(" DH: gf_encryptDH using sk algo: %s, Keysize: %d",
        dhimpl->m_skAlgo.c_str(), dhimpl->m_keySize);

  unsigned char *ciphertext =
      new unsigned char[len + 50];  // give enough room for padding
  int outlen, tmplen;
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);

  int ret = -123;

  const EVP_CIPHER *cipherFunc = dhimpl->getCipherFunc();

  // init openssl cipher context
  if (dhimpl->m_skAlgo == "AES") {
    int keySize = dhimpl->m_keySize > 128 ? dhimpl->m_keySize / 8 : 16;
    ret = EVP_EncryptInit_ex(&ctx, cipherFunc, NULL,
                             (unsigned char *)dhimpl->m_key,
                             (unsigned char *)dhimpl->m_key + keySize);
  } else if (dhimpl->m_skAlgo == "Blowfish") {
    int keySize = dhimpl->m_keySize > 128 ? dhimpl->m_keySize / 8 : 16;
    ret = EVP_EncryptInit_ex(&ctx, cipherFunc, NULL, NULL,
                             (unsigned char *)dhimpl->m_key + keySize);
    LOGDH("DHencrypt: init BF ret %d", ret);
    EVP_CIPHER_CTX_set_key_length(&ctx, keySize);
    LOGDH("DHencrypt: BF keysize is %d", keySize);
    ret = EVP_EncryptInit_ex(&ctx, NULL, NULL, (unsigned char *)dhimpl->m_key,
                             NULL);
  } else if (dhimpl->m_skAlgo == "DESede") {
    ret = EVP_EncryptInit_ex(&ctx, cipherFunc, NULL,
                             (unsigned char *)dhimpl->m_key,
                             (unsigned char *)dhimpl->m_key + 24);
  }

  LOGDH(" DHencrypt: init ret %d", ret);

  if (!EVP_EncryptUpdate(&ctx, ciphertext, &outlen, cleartext, len)) {
    LOGDH(" DHencrypt: enc update ret NULL");
    return NULL;
  }
  /* Buffer passed to EVP_EncryptFinal() must be after data just
   * encrypted to avoid overwriting it.
   */
  tmplen = 0;

  if (!EVP_EncryptFinal_ex(&ctx, ciphertext + outlen, &tmplen)) {
    LOGDH("DHencrypt: enc final ret NULL");
    return NULL;
  }

  outlen += tmplen;

  ret = EVP_CIPHER_CTX_cleanup(&ctx);

  LOGDH("DHencrypt: in len is %d, out len is %d", len, outlen);

  *retLen = outlen;
  return ciphertext;
}

unsigned char *gf_decryptDH(void *dhCtx, const unsigned char *cleartext,
                            int len, int *retLen) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  // Validation
  if (cleartext == NULL || len < 1 || retLen == NULL) {
    return NULL;
  }

  LOGDH(" DH: gf_encryptDH using sk algo: %s, Keysize: %d",
        dhimpl->m_skAlgo.c_str(), dhimpl->m_keySize);

  unsigned char *ciphertext =
      new unsigned char[len + 50];  // give enough room for padding
  int outlen, tmplen;
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);

  int ret = -123;

  const EVP_CIPHER *cipherFunc = dhimpl->getCipherFunc();

  // init openssl cipher context
  if (dhimpl->m_skAlgo == "AES") {
    int keySize = dhimpl->m_keySize > 128 ? dhimpl->m_keySize / 8 : 16;
    ret = EVP_DecryptInit_ex(&ctx, cipherFunc, NULL,
                             (unsigned char *)dhimpl->m_key,
                             (unsigned char *)dhimpl->m_key + keySize);
  } else if (dhimpl->m_skAlgo == "Blowfish") {
    int keySize = dhimpl->m_keySize > 128 ? dhimpl->m_keySize / 8 : 16;
    ret = EVP_DecryptInit_ex(&ctx, cipherFunc, NULL, NULL,
                             (unsigned char *)dhimpl->m_key + keySize);
    LOGDH("DHencrypt: init BF ret %d", ret);
    EVP_CIPHER_CTX_set_key_length(&ctx, keySize);
    LOGDH("DHencrypt: BF keysize is %d", keySize);
    ret = EVP_DecryptInit_ex(&ctx, NULL, NULL, (unsigned char *)dhimpl->m_key,
                             NULL);
  } else if (dhimpl->m_skAlgo == "DESede") {
    ret = EVP_DecryptInit_ex(&ctx, cipherFunc, NULL,
                             (unsigned char *)dhimpl->m_key,
                             (unsigned char *)dhimpl->m_key + 24);
  }

  LOGDH(" DHencrypt: init ret %d", ret);

  if (!EVP_DecryptUpdate(&ctx, ciphertext, &outlen, cleartext, len)) {
    LOGDH(" DHencrypt: enc update ret NULL");
    return NULL;
  }
  /* Buffer passed to EVP_EncryptFinal() must be after data just
   * encrypted to avoid overwriting it.
   */
  tmplen = 0;

  if (!EVP_DecryptFinal_ex(&ctx, ciphertext + outlen, &tmplen)) {
    LOGDH("DHencrypt: enc final ret NULL");
    return NULL;
  }

  outlen += tmplen;

  ret = EVP_CIPHER_CTX_cleanup(&ctx);

  LOGDH("DHencrypt: in len is %d, out len is %d", len, outlen);

  *retLen = outlen;
  return ciphertext;
}

// CacheableBytesPtr decrypt(const uint8_t * ciphertext, int len) {
//  LOGDH("DH: Used unimplemented decrypt!");
//  return NULL;
//}

bool gf_verifyDH(void *dhCtx, const char *subject,
                 const unsigned char *challenge, int challengeLen,
                 const unsigned char *response, int responseLen, int *reason) {
  DHImpl *dhimpl = reinterpret_cast<DHImpl *>(dhCtx);

  LOGDH(" In Verify - looking for subject %s", subject);

  EVP_PKEY *evpkey = NULL;
  X509 *cert = NULL;

  char *certsubject = NULL;

  int32_t count = static_cast<int32_t>(dhimpl->m_serverCerts.size());
  if (count == 0) {
    *reason = DH_ERR_NO_CERTIFICATES;
    return false;
  }

  for (int item = 0; item < count; item++) {
    certsubject = X509_NAME_oneline(
        X509_get_subject_name(dhimpl->m_serverCerts[item]), NULL, 0);

    // Ignore first letter for comparision, openssl adds / before subject name
    // e.g. /CN=gemfire1
    if (strcmp((const char *)certsubject + 1, subject) == 0) {
      evpkey = X509_get_pubkey(dhimpl->m_serverCerts[item]);
      cert = dhimpl->m_serverCerts[item];
      LOGDH("Found subject [%s] in stored certificates", certsubject);
      break;
    }
  }

  if (evpkey == NULL || cert == NULL) {
    *reason = DH_ERR_SUBJECT_NOT_FOUND;
    LOGDH("Certificate not found!");
    return false;
  }

  int rsalen ATTR_UNUSED = RSA_size(evpkey->pkey.rsa);

  LOGDH("Challenge response length is %d, rsalen is %d\n", responseLen, rsalen);

  if (cert->sig_alg->algorithm != NULL) {
    LOGDH("algo %s -- %s \n", cert->sig_alg->algorithm->sn,
          cert->sig_alg->algorithm->ln);
  } else {
    LOGDH("algo is null \n");
  }

  LOGDH("after algo name in DHimp = %s\n", cert->name);
  const EVP_MD *signatureDigest = EVP_get_digestbyobj(cert->sig_alg->algorithm);
  LOGDH("after EVP_get_digestbyobj  :  err(%d): %s", ERR_get_error(),
        ERR_error_string(ERR_get_error(), NULL));
  EVP_MD_CTX signatureCtx;
  EVP_MD_CTX_init(&signatureCtx);

  int result1 = EVP_VerifyInit_ex(&signatureCtx, signatureDigest, NULL);
  LOGDH("after EVP_VerifyInit_ex ret %d : err(%d): %s", result1,
        ERR_get_error(), ERR_error_string(ERR_get_error(), NULL));
  LOGDH(" Result of VerifyInit is %s \n", ERR_lib_error_string(result1));
  LOGDH(" Result of VerifyInit is %s \n", ERR_func_error_string(result1));
  LOGDH(" Result of VerifyInit is %s \n", ERR_reason_error_string(result1));

  LOGDH(" Result of VerifyInit is %d", result1);

  int result2 = EVP_VerifyUpdate(&signatureCtx, challenge, challengeLen);
  LOGDH(" Result of VerifyUpdate is %d", result2);

  int result3 = EVP_VerifyFinal(&signatureCtx, response, responseLen, evpkey);
  LOGDH(" Result of VerifyFinal is %d", result3);

  bool result = (result1 == 1 && result2 == 1 && result3 == 1);

  EVP_MD_CTX_cleanup(&signatureCtx);

  if (result == false) {
    *reason = DH_ERR_INVALID_SIGN;
  }

  return result;
}

int DH_PUBKEY_set(DH_PUBKEY **x, EVP_PKEY *pkey) {
  DH_PUBKEY *pk = NULL;
  X509_ALGOR *a;
  ASN1_OBJECT *o;
  unsigned char *s, *p = NULL;
  int i;
  ASN1_INTEGER *asn1int = NULL;

  if (x == NULL) return (0);

  if ((pk = DH_PUBKEY_new()) == NULL) goto err;
  a = pk->algor;

  LOGDH(" key type for OBJ NID is %d", pkey->type);

  /* set the algorithm id */
  if ((o = OBJ_nid2obj(pkey->type)) == NULL) goto err;
  ASN1_OBJECT_free(a->algorithm);
  a->algorithm = o;

  /* Set the parameter list */
  if (!pkey->save_parameters || (pkey->type == EVP_PKEY_RSA)) {
    if ((a->parameter == NULL) || (a->parameter->type != V_ASN1_NULL)) {
      ASN1_TYPE_free(a->parameter);
      if (!(a->parameter = ASN1_TYPE_new())) {
        X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
        goto err;
      }
      a->parameter->type = V_ASN1_NULL;
    }
  } else if (pkey->type == EVP_PKEY_DH) {
    unsigned char *pp;
    DH *dh;

    dh = pkey->pkey.dh;
    ASN1_TYPE_free(a->parameter);
    if ((i = i2d_DHparams(dh, NULL)) <= 0) goto err;
    if (!(p = reinterpret_cast<unsigned char *>(OPENSSL_malloc(i)))) {
      X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
      goto err;
    }
    pp = p;
    i2d_DHparams(dh, &pp);
    if (!(a->parameter = ASN1_TYPE_new())) {
      OPENSSL_free(p);
      X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
      goto err;
    }
    a->parameter->type = V_ASN1_SEQUENCE;
    if (!(a->parameter->value.sequence = ASN1_STRING_new())) {
      OPENSSL_free(p);
      X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
      goto err;
    }
    if (!ASN1_STRING_set(a->parameter->value.sequence, p, i)) {
      OPENSSL_free(p);
      X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
      goto err;
    }
    OPENSSL_free(p);
  } else if (1) {
    X509err(X509_F_X509_PUBKEY_SET, X509_R_UNSUPPORTED_ALGORITHM);
    goto err;
  }

  asn1int = BN_to_ASN1_INTEGER(pkey->pkey.dh->pub_key, NULL);
  if ((i = i2d_ASN1_INTEGER(asn1int, NULL)) <= 0) goto err;
  if ((s = reinterpret_cast<unsigned char *>(OPENSSL_malloc(i + 1))) == NULL) {
    X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
    goto err;
  }
  p = s;
  i2d_ASN1_INTEGER(asn1int, &p);
  if (!M_ASN1_BIT_STRING_set(pk->public_key, s, i)) {
    X509err(X509_F_X509_PUBKEY_SET, ERR_R_MALLOC_FAILURE);
    goto err;
  }
  /* Set number of unused bits to zero */
  pk->public_key->flags &= ~(ASN1_STRING_FLAG_BITS_LEFT | 0x07);
  pk->public_key->flags |= ASN1_STRING_FLAG_BITS_LEFT;

  OPENSSL_free(s);

  if (*x != NULL) DH_PUBKEY_free(*x);

  *x = pk;

  return 1;
err:
  if (asn1int != NULL) ASN1_INTEGER_free(asn1int);
  if (pk != NULL) DH_PUBKEY_free(pk);
  return 0;
}

EVP_PKEY *DH_PUBKEY_get(DH_PUBKEY *key) {
  EVP_PKEY *ret = NULL;
  long j;
  int type;
  const unsigned char *p;
  const unsigned char *cp;
  X509_ALGOR *a;
  ASN1_INTEGER *asn1int = NULL;

  if (key == NULL) goto err;

  if (key->pkey != NULL) {
    CRYPTO_add(&key->pkey->references, 1, CRYPTO_LOCK_EVP_PKEY);
    return (key->pkey);
  }

  if (key->public_key == NULL) goto err;

  type = OBJ_obj2nid(key->algor->algorithm);

  LOGDH("DHPUBKEY type is %d", type);

  if ((ret = EVP_PKEY_new()) == NULL) {
    X509err(X509_F_X509_PUBKEY_GET, ERR_R_MALLOC_FAILURE);
    goto err;
  }
  ret->type = EVP_PKEY_type(type);

  LOGDH(" DHPUBKEY evppkey type is %d", ret->type);

  /* the parameters must be extracted before the public key */

  a = key->algor;

  if (ret->type == EVP_PKEY_DH) {
    if (a->parameter && (a->parameter->type == V_ASN1_SEQUENCE)) {
      if ((ret->pkey.dh = DH_new()) == NULL) {
        X509err(X509_F_X509_PUBKEY_GET, ERR_R_MALLOC_FAILURE);
        goto err;
      }
      cp = p = a->parameter->value.sequence->data;
      j = a->parameter->value.sequence->length;
      if (!d2i_DHparams(&ret->pkey.dh, &cp, j)) goto err;
    }
    ret->save_parameters = 1;
  }

  p = key->public_key->data;
  j = key->public_key->length;

  asn1int = d2i_ASN1_INTEGER(NULL, &p, j);
  LOGDH("after d2i asn1 integer ptr is %p", asn1int);

  ret->pkey.dh->pub_key = ASN1_INTEGER_to_BN(asn1int, NULL);
  LOGDH(" after asn1int to bn ptr is %p", ret->pkey.dh->pub_key);

  key->pkey = ret;
  CRYPTO_add(&ret->references, 1, CRYPTO_LOCK_EVP_PKEY);

  if (asn1int != NULL) ASN1_INTEGER_free(asn1int);
  return (ret);
err:
  if (asn1int != NULL) ASN1_INTEGER_free(asn1int);
  if (ret != NULL) EVP_PKEY_free(ret);
  return (NULL);
}
