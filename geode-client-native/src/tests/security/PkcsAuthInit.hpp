#ifndef _GEMFIRE__PKCSAUTHINIT__HPP
#define _GEMFIRE__PKCSAUTHINIT__HPP
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


#include "AuthInitialize.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/pkcs12.h>
//#include <unistd.h>
//#include <netinet/in.h>
//#include <fcntl.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#define KSSL_H 1
//#define OPENSSL_NO_KRB5 1
#include <openssl/ssl.h>


/**
 * @file
 */
const char KEYSTORE_FILE_PATH1[] = "security-keystorepath";

const char KEYSTORE_ALIAS1[] = "security-alias";

const char KEYSTORE_PASSWORD1[] = "security-keystorepass";

const char SIGNATURE_DATA1[] = "security-signature";

namespace gemfire {

/**
 * @class PKCSAuthInit Implementation PKCSAuthInit.hpp
 * PKCSAuthInit API for getCredentials.
 * The PKCSAuthInit class derives from AuthInitialize base class.
 * It uses the provided alias, password and corresponding keystore to obtain the private key and
 * encrypts data. This data is sent to server for authentication.
 *
 */


class PKCSAuthInitInternal : public AuthInitialize {
/**
 * @brief public methods
 */
public:

 /**
  * @brief constructor
  */
  PKCSAuthInitInternal(bool makeString = false):m_stringCredentials(makeString){}

 /**
  * @brief destructor
  */
 ~PKCSAuthInitInternal(){}


  /**@brief initialize with the given set of security properties
   * and return the credentials for the client as properties.
   * @param props the set of security properties provided to the
   * <code>DistributedSystem.connect</code> method
   * @param server it is the ID of the current endpoint.
   * The format expected is "host:port".
   * @returns the credentials to be used for the given <code>server</code>
   */
  PropertiesPtr getCredentials(PropertiesPtr& securityprops, const char *server);

  /**
   * @brief Invoked before the cache goes down.
   */
  void close() {
    return ;
  }

 /**
  * @brief private members
  */

private:

  bool m_stringCredentials;

};
};
#endif //_GEMFIRE__PKCSAUTHINIT__HPP

