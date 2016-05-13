#!/bin/bash

if [ -z ${GFCPP:-} ]; then
  echo GFCPP is not set.
  exit 1
fi
if [ -z ${GEMFIRE:-} ]; then
  echo GEMFIRE is not set.
  exit 1
fi
if [ -z ${OPENSSL:-} ]; then
  echo Warning: OPENSSL is not set.
  echo If OpenSSL libraries are not found in default library search path then Security example will fail.
  echo
  echo Press Enter to continue
  read resp
fi

exname='invalidoption'

if [ -f cpp/$1 ]
then
  exname=$1
else
  echo Please select a GemFire C++ QuickStart example to run.
  echo
  echo 1. BasicOperations
  echo 2. DataExpiration
  echo 3. LoaderListenerWriter
  echo 4. RegisterInterest
  echo 5. RemoteQuery
  echo 6. HA Cache
  echo 7. Exceptions
  echo 8. DurableClient
  echo 9. Security
  echo 10.PutAllGetAllOperations
  echo 11.Continuous Query
  echo 12.Execute Functions
  echo 13.DistributedSystem
  echo 14.PoolWithEndpoints
  echo 15.PoolRemoteQuery
  echo 16.Pool Continuous Query
  echo 17.Delta
  echo 18.Multiuser Security
  echo 19.RefIDExample
  echo 20.Transactions
  echo 21.TransactionsXA
  echo 22.PdxRemoteQuery
  echo 23.PdxSerializer
  echo 24.PdxInstance
  echo 25.PdxAutoSerializer
  echo 26.Quit
  echo

  while [ "${exname}" == "invalidoption" ]
  do
    read -p "Enter option: " option
    case "${option}" in
      "1")
        exname='BasicOperations'
      ;;
      "2")
        exname='DataExpiration'
      ;;
      "3")
        exname='LoaderListenerWriter'
      ;;
      "4")
        exname='RegisterInterest'
      ;;
      "5")
        exname='RemoteQuery'
      ;;
      "6")
        exname='HACache'
      ;;
      "7")
        exname='Exceptions'
      ;;
      "8")
        exname='DurableClient'
      ;;
      "9")
        exname='Security'
      ;;
      "10")
        exname='PutAllGetAllOperations'
      ;;  
      "11")
        exname='CqQuery'
      ;;
      "12")
              exname='ExecuteFunctions'
      ;;
      "13")
        exname='DistributedSystem'
      ;;
      "14")
        exname='PoolWithEndpoints'
      ;;  
      "15")
        exname='PoolRemoteQuery'
      ;;
      "16")
        exname='PoolCqQuery'
      ;;
      "17")
        exname='Delta'
      ;;
      "18")
        exname='MultiuserSecurity'
      ;;
	  "19")
        exname='RefIDExample'
      ;;
	  "20")
        exname='Transactions'
		;;
          "21")
        exname='TransactionsXA'
                ;;
	  "22")
        exname='PdxRemoteQuery'
		;;
	  "23")
        exname='PdxSerializer'
		;;
	  "24")
        exname='PdxInstance'
      ;;
	  "25")
        exname='PdxAutoSerializer'
      ;;
      "26")
        exname='Quit'
	exit
      ;;
    esac
  done  
fi

echo Running GemFire C++ QuickStart example ${exname} ...

export CLASSPATH="${CLASSPATH}:../lib/javaobject.jar:${GEMFIRE}/lib/gfSecurityImpl.jar"
export PATH="${PATH}:${GEMFIRE}/bin"

if [ ! -d gfecs ]
then
  mkdir gfecs
fi


if [ "${exname}" != "Security" ]
  then
    if [ "${exname}" != "MultiuserSecurity" ]
      then
        if [[ "${exname}" != "PoolRemoteQuery" && "${exname}" != "Delta" ]]
          then
            cacheserver start cache-xml-file=../XMLs/server${exname}.xml mcast-port=35673 -dir=gfecs
        else
          gemfire start-locator -port=34756 -dir=gfecs 
          cacheserver start cache-xml-file=../XMLs/server${exname}.xml mcast-port=0 -dir=gfecs locators=localhost:34756
        fi
    else
      cacheserver start cache-xml-file=../XMLs/server${exname}.xml mcast-port=35673 -dir=gfecs security-client-authenticator=templates.security.DummyAuthenticator.create security-authz-xml-uri=../XMLs/authz-dummy.xml security-client-accessor=templates.security.XmlAuthorization.create
  fi
else
   cacheserver start cache-xml-file=../XMLs/server${exname}.xml mcast-port=35673 -dir=gfecs security-client-authenticator=templates.security.PKCSAuthenticator.create security-publickey-filepath=../keystore/publickeyfile security-publickey-pass=gemfire security-authz-xml-uri=../XMLs/authz-pkcs.xml security-client-accessor=templates.security.XmlAuthorization.create
fi

if [ "${exname}" == "DistributedSystem" ]
then
  if [ ! -d gfecs2 ]
  then
    mkdir gfecs2
  fi
  cacheserver start cache-xml-file=../XMLs/server${exname}2.xml mcast-port=35674 -dir=gfecs2
fi


if [ "${exname}" == "HACache"  ]
then
  if [ ! -d gfecs2 ]
  then
    mkdir gfecs2
  fi
  cacheserver start cache-xml-file=../XMLs/server${exname}2.xml mcast-port=35673 -dir=gfecs2
fi

if [ "${exname}" == "ExecuteFunctions" ]
then
  if [ ! -d gfecs2 ]
  then
    mkdir gfecs2
  fi
  cacheserver start cache-xml-file=../XMLs/server${exname}2.xml mcast-port=35673 -dir=gfecs2
fi
success=0
if [ "${exname}" != "Security" ]
then
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${OPENSSL}/lib
else
  echo "starting client security less"
fi

$DEBUGGER cpp/${exname}
success=$?

cacheserver stop -dir=gfecs

if [[ "${exname}" == "PoolRemoteQuery" || "${exname}" == "Delta" ]]
   then
    gemfire stop-locator -port=34756 -dir=gfecs
fi

if [ "${exname}" == "HACache" -o "${exname}" == "DistributedSystem" ]
then
  echo CacheServer 2 stopped
  cacheserver stop -dir=gfecs2
fi

if [ "${exname}" == "ExecuteFunctions" ]
then
  cacheserver stop -dir=gfecs2
fi

if [ $success -ne "0" ]; then
  exit 1
fi

echo Finished example ${exname}.

