/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkBB.hpp
  * @since   1.0
  * @version 1.0
  * @see
  */

#ifndef __FWK_BB_HPP__
#define __FWK_BB_HPP__

#include <gfcpp/gf_base.hpp>
#include "fwklib/FwkLog.hpp"

#include <vector>
#include <string>
#include <sstream>

// ----------------------------------------------------------------------------

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

//    #define BB_START_TAG              "<s>"
#define BB_ID_TAG "<i>"
#define BB_COMMAND_TAG "<c>"
#define BB_PARAMETER_TAG "<p>"
#define BB_RESULT_TAG "<r>"
//    #define BB_END_TAG                "<e>"

#define BB_CLEAR_COMMAND "C"           //"clear"
#define BB_DUMP_COMMAND "d"            //"dump"
#define BB_GET_COMMAND "g"             //"get"
#define BB_SET_COMMAND "s"             //"set"
#define BB_ADD_COMMAND "A"             //"add"
#define BB_SUBTRACT_COMMAND "S"        //"subtract"
#define BB_INCREMENT_COMMAND "I"       //"increment"
#define BB_DECREMENT_COMMAND "D"       //"decrement"
#define BB_ZERO_COMMAND "z"            //"zero"
#define BB_SET_IF_GREATER_COMMAND "G"  //"setIfGreater"
#define BB_SET_IF_LESS_COMMAND "L"     //"setIfLess"
#define BB_SET_ACK_COMMAND "a"         //"ack"

// ----------------------------------------------------------------------------

/** @class FwkBBMessage
  * @brief Framework BB message
  *
  * Message stream format
  * @verbatim
      <start><id>IIII<c>CCCC<p>PPPP<p>PPPP<p>PPPP<r>RRRR<end>

        <start> start tag of message
        <id> id of operation
        <c> command value
        <p> parameter value
        <r> result value
        <end> end tag of message
    @endverbatim
  */
class FwkBBMessage {
 public:
  FwkBBMessage(const char* cmd) : m_cmd(cmd) {}
  FwkBBMessage() {}
  virtual ~FwkBBMessage() {}

  /** @brief clear message data
    */
  void clear() {
    m_parameterVector.clear();
    m_id.clear();
    m_cmd.clear();
    m_result.clear();
  }

  /** @brief pass to data to parse onReceive message
    * @param psData data pointer, not null terminated
    * @param dataSize data size of message
    * @retval true = Success, false = Failed
    */
  void fromMessageStream(std::string data) {
    //        FWKINFO( "FwkBBMessage::fromMessageStream: " << data );
    char* str = (char*)(data.c_str());
    char* tag = strstr(str, BB_ID_TAG);
    if (tag == NULL) {
      FWKEXCEPTION("Invalid BB message: " << data);
    }
    tag += 3;
    int32_t len = (int32_t)strcspn(tag, "<");
    std::string id(tag, len);
    setId(id);

    tag = strstr(str, BB_COMMAND_TAG);
    if (tag == NULL) {
      FWKEXCEPTION("Invalid BB message: " << data);
    }
    tag += 3;
    len = (int32_t)strcspn(tag, "<");
    std::string cmd(tag, len);
    setCommand(cmd);

    tag = strstr(str, BB_RESULT_TAG);
    if (tag != NULL) {
      tag += 3;
      len = (int32_t)strcspn(tag, "<");
      std::string result(tag, len);
      setResult(result);
    }

    tag = strstr(str, BB_PARAMETER_TAG);
    while (tag != NULL) {
      tag += 3;
      len = (int32_t)strcspn(tag, "<");
      std::string param(tag, len);
      addParameter(param);
      tag = strstr(tag, BB_PARAMETER_TAG);
    }
  }

  /** @brief get data stream to send
    * @param sStream data stream
    * @retval true = Success, false = Failed
    */
  std::string& toMessageStream() {
    m_stream.clear();
    std::ostringstream osMessage;
    if (m_id.empty()) {
      FWKEXCEPTION("Invalid BB Message, id not set.");
    }
    if (m_cmd.empty()) {
      FWKEXCEPTION("Invalid BB Message, command not set.");
    }

    osMessage << BB_ID_TAG << m_id << BB_COMMAND_TAG << m_cmd;
    if (m_parameterVector.size() > 0) {
      std::vector<std::string>::iterator it = m_parameterVector.begin();
      while (it != m_parameterVector.end()) {
        osMessage << BB_PARAMETER_TAG << *it;
        it++;
      }
    }
    if (!m_result.empty()) osMessage << BB_RESULT_TAG << m_result;

    m_stream.append(osMessage.str());
    return m_stream;
  }

  /** @brief set Id of message
    * @param sId id of message
    */
  void setId(std::string id) { m_id = id; };

  /** @brief set command of message
    * @param sCommand command of message
    */
  void setCommand(std::string cmd) { m_cmd = cmd; };

  /** @brief set result of message
    * @param sResult result of message
    */
  void setResult(std::string result) { m_result = result; };

  /** @brief add parameter value to message
    * @param sParameter parameter of message
    */
  void addParameter(std::string parameter) {
    m_parameterVector.push_back(parameter);
  };

  /** @brief get id of message
    * @retval id of message
    */
  std::string getId() { return m_id; };

  /** @brief get command of message
    * @retval command of message
    */
  std::string getCommand() { return m_cmd; };
  const char getCmdChar() { return m_cmd.at(0); }

  /** @brief get result of message
    * @retval result of message
    */
  std::string getResult() { return m_result; };

  /** @brief get parameter of message
    * @retval parameter of message
    */
  std::string getParameter(unsigned short index) {
    std::string value;
    if (index < m_parameterVector.size()) value = m_parameterVector[index];

    return value;
  }

 private:
  std::vector<std::string> m_parameterVector;
  std::string m_id;
  std::string m_cmd;
  std::string m_result;
  std::string m_stream;
};

// ----------------------------------------------------------------------------

}  // namespace  testframework
}  // namepace gemfire

// ----------------------------------------------------------------------------

#endif  // __FWK_BB_HPP__
