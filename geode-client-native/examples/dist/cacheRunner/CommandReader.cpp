/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * @file CommandReader.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#include "CommandReader.hpp"
#include <string.h> 
#include <stdio.h>

// ----------------------------------------------------------------------------

CommandReader::CommandReader(void) :
  m_sCommand("")
{
}
//----------------------------------------------------------------------------

CommandReader::CommandReader( std::string command ) :
  m_sCommand(command)
{
  parseStringToList(m_sCommand, m_commandList);
}
// ----------------------------------------------------------------------------

CommandReader::~CommandReader()
{
}

// ----------------------------------------------------------------------------
/**
 * clears current command list and command string
 */
// ----------------------------------------------------------------------------

void CommandReader::clearCommand( )
{
  m_commandList.clear();
  m_sCommand.clear();
}

// ----------------------------------------------------------------------------
/**
 * read command line from stdin up to 80 characters and stores value to 
 * current command list and current command string
 */
// ----------------------------------------------------------------------------

void CommandReader::readCommandLineFromStdin( )
{
	char szLine[1024];

  // clear the current command 
  clearCommand();

  // read up to 80 characters from stdin
  fgets(szLine, 1022, stdin);
  char *p;
  if( (p = strchr( szLine, '\n' )) != NULL )
    *p = '\0';
  if (strlen(szLine) >= 1){
    m_sCommand = szLine;
	  parseStringToList(m_sCommand, m_commandList);
  }
}

// ----------------------------------------------------------------------------
/**
  * compares a command string to the current command 
  * case is compared, length is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isCommand( const char* pszCommand )
{
  return isToken(pszCommand);
}

// ----------------------------------------------------------------------------
/**
  * compares a command string to the current command
  * case is compared, length of input string is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isCommandStartsWith( const char* pszCommand)
{
  return isTokenStartsWith(pszCommand);
}

// ----------------------------------------------------------------------------
/**
  * compares a command string to the current command
  * no case is compared, length is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isCommandNoCase( const char* pszCommand )
{
  return isTokenNoCase(pszCommand);
}

// ----------------------------------------------------------------------------
/**
  * compares a command string to the current token 
  * no case is compared, length of input is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isCommandStartsWithNoCase( const char* pszCommand )
{
  return isTokenStartsWithNoCase(pszCommand);
}

// ----------------------------------------------------------------------------
/**
  * compares a token string to the current token
  * case is compared, length is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isToken( const char* pszToken, int iIndex)
{
  bool bSuccess = false;

  if (iIndex < (int)m_commandList.size() && pszToken){
    if (!strcmp(pszToken, m_commandList[iIndex].c_str()))
      bSuccess = true;
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
/**
  * compares a token string to the current token
  * case is compared, length of input string is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isTokenStartsWith( const char* pszToken, int iIndex )
{
  bool bSuccess = false;

  if (iIndex < (int)m_commandList.size() && pszToken){
    if (!strncmp(pszToken, m_commandList[iIndex].c_str(), strlen(pszToken)))
    {
      bSuccess = true;
    }
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
/**
  * compares a token string to the current token 
  * no case is compared, length is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isTokenNoCase( const char* pszToken, int iIndex )
{
  bool bSuccess = false;

  if (iIndex < (int)m_commandList.size() && pszToken){
#ifdef _WIN32
    if (!stricmp(pszToken, m_commandList[iIndex].c_str()))
#else
    if (!strcasecmp(pszToken, m_commandList[iIndex].c_str()))
#endif
      bSuccess = true;
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
/**
  * compares a token string to the current token
  * no case is compared, length of input string is compared
  * return true if success, false if unsucessful
  */
// ----------------------------------------------------------------------------

bool CommandReader::isTokenStartsWithNoCase(const char* pszToken, int iIndex )
{
  bool bSuccess = false;

  if (iIndex < (int)m_commandList.size() && pszToken){
#ifdef _WIN32
    if (!strnicmp(pszToken, m_commandList[iIndex].c_str(), strlen(pszToken)))
#else
    if (!strncasecmp(pszToken, m_commandList[iIndex].c_str(), strlen(pszToken)))
#endif
    {
      bSuccess = true;
    }
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
/**
 * return parameter from command line input
 */
// ----------------------------------------------------------------------------

std::string CommandReader::getTokenString( int iIndex ,bool isQuery)
{
  std::string sParameter;
  
  
  if(isQuery) {
    for(int queryCnt = iIndex; queryCnt < (int)m_commandList.size(); queryCnt++)
    {
      sParameter += m_commandList[queryCnt]; 
      sParameter += ' ';
    }
  } else {
    if (iIndex < (int)m_commandList.size()){
      sParameter = m_commandList[iIndex];
    }
  }
  return sParameter;
}

// ----------------------------------------------------------------------------
/**
 * returns the number of tokens from command line input
 */
// ----------------------------------------------------------------------------

int CommandReader::getNumberOfTokens( )
{
  return (int)m_commandList.size();
}

// ----------------------------------------------------------------------------
/**
  * returns the current command line text
  */
// ----------------------------------------------------------------------------

std::string CommandReader::getCommandString( )
{
  return m_sCommand;
}

// ----------------------------------------------------------------------------
/**
  * parses a text string delimited by spaces into a vector array
  */
// ----------------------------------------------------------------------------

void CommandReader::parseStringToList( const std::string& sText, tCommandReaderList& commandList )
{
  commandList.clear();  // make sure output string is cleared

  size_t size = sText.size();

  if (size){ // make sure input string has data
    std::string sTmp;
    char        cChar = 0;
    size_t      index = 0;


    while(index < size){
      cChar = sText[index];

      if (cChar == ' ') {
	  if (sTmp.size())  // make sure there is data
            commandList.push_back(sTmp);
          sTmp.clear();
      } else {
          sTmp += cChar;
      }
      index++;
    }

    if (sTmp.size()) // add the last string if any
      commandList.push_back(sTmp);
  }
}

// ----------------------------------------------------------------------------
/** helper function to compare strings
 */
// ----------------------------------------------------------------------------

bool CommandReader::startsWith(const char* pszToken, const char* pszText)
{
  bool bSuccess = false;

  if (pszToken && pszText){
    if (!strncmp(pszToken, pszText, strlen(pszText)))
    {
      bSuccess = true;
    }
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
