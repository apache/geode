/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_OUTPUTFORMATTER_HPP_
#define _GFAS_OUTPUTFORMATTER_HPP_

#include <ostream>
#include <streambuf>
#include <fstream>
#include <string>

namespace gemfire {
namespace pdx_auto_serializer {
/** The default character to use for indentation. */
const char DefaultIndentChar = ' ';

/** The default indentation size. */
const int DefaultIndentSize = 2;

/**
 * A<code>std::streambuf</code> class that formats the buffer with
 * appropriate indentation.
 *
 * It derives from the standard <code>std::streambuf</code> class and
 * turns off buffering while delegating the actual task to another
 * <code>std::streambuf</code> contained in it. This is required so that
 * the <code>OutputFormatStreamBuf::overflow</code> method is called for
 * every write.
 */
class OutputFormatStreamBuf : public std::streambuf {
 public:
  /** Default constructor. */
  OutputFormatStreamBuf();

  /**
   * Initialize the buffer with the given <code>std::streambuf</code>
   * and given indentation character, indentation size.
   *
   * @param buf The actual <code>std::streambuf</code> to be used for
   *            buffering.
   * @param indentChar The character to use for indentation of each line.
   * @param indentSize The size for each level of indentation.
   */
  void init(std::streambuf* buf, char indentChar, int indentSize);

  /**
   * Change the indentation character.
   *
   * @param indentChar The new indentation character.
   */
  void setIndentChar(char indentChar);

  /**
   * Change the number of characters for each indentation level.
   *
   * @param indentSize The new size for indentation.
   */
  void setIndentSize(int indentSize);

  /** Increase the current indentation level by one. */
  void increaseIndent();

  /** Decrease the current indentation level by one. */
  void decreaseIndent();

  /**
   * Get the current indentation level.
   *
   * @return The current indentation level.
   */
  virtual int getIndent() const;

  /**
   * Set the current indentation level.
   *
   * @param indentLevel The indentation level to set.
   */
  virtual void setIndent(int indentLevel);

  /** Virtual destructor. */
  virtual ~OutputFormatStreamBuf();

 protected:
  /**
   * The overriden <code>std::streambuf::overflow</code> method that
   * inserts the indentation characters when starting a new line.
   *
   * Note that buffering for this <code>streambuf</code> is turned off so
   * that this is called for every write and the contained
   * <code>m_buf</code> is the one actually used.
   *
   * @param c The character to be written.
   */
  virtual int overflow(int c);

  /** Overriden <code>std::streambuf::sync</code> method. */
  virtual int sync();

  /**
   * The contained <code>streambuf</code> that does the actual work of
   * buffering/writing.
   */
  std::streambuf* m_buf;

  /** The indentation character to be used. */
  char m_indentChar;

  /** The size of indentation in each level. */
  int m_indentSize;

  /** The current indentation level. */
  int m_indentLevel;

  /** True when a newline has just been encountered. */
  bool m_newLine;

  /** True when an opening brace has just been encountered. */
  bool m_openBrace;
};

/**
 * A<code>std::ostream</code> class that writes the output to a given file
 * and formats the output with appropriate indentation.
 */
class OutputFormatter : public std::ostream {
 public:
  /** The default constructor */
  OutputFormatter();

  /**
   * Open a given file for writing (just like <code>std::ofstream</code>
   * in the given mode and use the provided indentation character and size.
   *
   * @param fileName Name of the file to open.
   * @param mode The mode to use when opening the file -- default is to
   *             truncate the file and open in write mode.
   * @param indentChar The character to use for indentation.
   * @param indentSize The number of characters to use for indentation.
   */
  virtual void open(const std::string& fileName,
                    ios_base::openmode mode = ios_base::out | ios_base::trunc,
                    char indentChar = DefaultIndentChar,
                    int indentSize = DefaultIndentSize);

  /**
   * Change the indentation character.
   *
   * @param indentChar The new indentation character.
   */
  virtual void setIndentChar(char indentChar);

  /**
   * Change the number of characters for each indentation level.
   *
   * @param indentSize The new size for indentation.
   */
  virtual void setIndentSize(int indentSize);

  /** Increase the current indentation level by one. */
  virtual void increaseIndent();

  /** Decrease the current indentation level by one. */
  virtual void decreaseIndent();

  /**
   * Get the current indentation level.
   *
   * @return The current indentation level.
   */
  virtual int getIndent() const;

  /**
   * Set the current indentation level.
   *
   * @param indentLevel The indentation level to set.
   */
  virtual void setIndent(int indentLevel);

  /**
   * Get the underlying name of output file.
   *
   * @return The path of the output file.
   */
  std::string getFileName() const;

  /** Overrides the <code>std::ostream::flush</code> method to flush the
   * output stream to the file.
   */
  virtual void flush();

  /**
   * Overrides the <code>std::ostream::close</code> method to close the
   * underlying stream and file.
   */
  virtual void close();

 protected:
  /** The underlying output file stream object. */
  std::ofstream m_ofstream;

  /** The name of the output file. */
  std::string m_fileName;

  /** The formatter to use for formatting the output. */
  OutputFormatStreamBuf m_streamBuf;
};
}
}

#endif  // _GFAS_OUTPUTFORMATTER_HPP_
