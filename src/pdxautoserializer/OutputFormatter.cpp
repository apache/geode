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
