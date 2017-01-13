/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "OutputFormatter.hpp"
#include <cstring>
#include <cerrno>

namespace gemfire {
namespace pdx_auto_serializer {
// OutputFormatStreamBuf method definitions

OutputFormatStreamBuf::OutputFormatStreamBuf()
    : m_buf(NULL),
      m_indentSize(0),
      m_indentLevel(0),
      m_newLine(true),
      m_openBrace(false) {
  std::streambuf::setp(NULL, NULL);
  std::streambuf::setg(NULL, NULL, NULL);
}

void OutputFormatStreamBuf::init(std::streambuf* buf, char indentChar,
                                 int indentSize) {
  m_buf = buf;
  m_indentChar = indentChar;
  m_indentSize = indentSize;
}

void OutputFormatStreamBuf::setIndentChar(char indentChar) {
  m_indentChar = indentChar;
}

void OutputFormatStreamBuf::setIndentSize(int indentSize) {
  m_indentSize = indentSize;
}

void OutputFormatStreamBuf::increaseIndent() { ++m_indentLevel; }

void OutputFormatStreamBuf::decreaseIndent() { --m_indentLevel; }

int OutputFormatStreamBuf::getIndent() const { return m_indentLevel; }

void OutputFormatStreamBuf::setIndent(int indentLevel) {
  m_indentLevel = indentLevel;
}

int OutputFormatStreamBuf::overflow(int c) {
  if (c != EOF) {
    if (m_newLine && c != '\n') {
      if (c != '#') {
        if (c == '}') {
          --m_indentLevel;
        }
        for (int i = 0; i < m_indentLevel * m_indentSize; ++i) {
          if (m_buf->sputc(m_indentChar) == EOF) {
            return EOF;
          }
        }
      }
      m_newLine = false;
    }
    if (c == '\n') {
      m_newLine = true;
      if (m_openBrace) {
        ++m_indentLevel;
      }
    }
    if (c == '{') {
      m_openBrace = true;
    } else {
      m_openBrace = false;
    }
    return m_buf->sputc(c);
  }
  return 0;
}

int OutputFormatStreamBuf::sync() { return m_buf->pubsync(); }

OutputFormatStreamBuf::~OutputFormatStreamBuf() {}

// OutputFormatter method definitions

OutputFormatter::OutputFormatter()
    : std::ostream(NULL), m_ofstream(), m_streamBuf() {
  m_streamBuf.init(m_ofstream.rdbuf(), DefaultIndentChar, DefaultIndentSize);
  std::ostream::init(&m_streamBuf);
}

void OutputFormatter::open(const std::string& fileName, ios_base::openmode mode,
                           char indentChar, int indentSize) {
  m_fileName = fileName;
  m_ofstream.open(fileName.c_str(), mode);
  if (!m_ofstream) {
    throw std::ios_base::failure(std::strerror(errno));
  }
  m_streamBuf.setIndentChar(indentChar);
  m_streamBuf.setIndentSize(indentSize);
}

void OutputFormatter::setIndentChar(char indentChar) {
  m_streamBuf.setIndentChar(indentChar);
}

void OutputFormatter::setIndentSize(int indentSize) {
  m_streamBuf.setIndentSize(indentSize);
}

void OutputFormatter::increaseIndent() { m_streamBuf.increaseIndent(); }

void OutputFormatter::decreaseIndent() { m_streamBuf.decreaseIndent(); }

int OutputFormatter::getIndent() const { return m_streamBuf.getIndent(); }

void OutputFormatter::setIndent(int indentLevel) {
  m_streamBuf.setIndent(indentLevel);
}

std::string OutputFormatter::getFileName() const { return m_fileName; }

void OutputFormatter::flush() {
  std::ostream::flush();
  m_ofstream.flush();
}

void OutputFormatter::close() {
  std::ostream::flush();
  m_ofstream.close();
}
}  // namespace pdx_auto_serializer
}  // namespace gemfire
