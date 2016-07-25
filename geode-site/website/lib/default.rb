# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# All files in the 'lib' directory will be loaded
# before nanoc starts compiling.
include Nanoc::Helpers::Rendering

require 'pandoc-ruby'
require 'htmlentities'

class PandocFilter < Nanoc::Filter
  identifier :pandoc
  type :text

  def run(content, params = {})
    ::PandocRuby.convert(content, 'smart', 'no-highlight', 'toc', :template => 'lib/pandoc.template')
  end
end

class FencedCodeBlock < Nanoc::Filter
  identifier :fenced_code_block

  def run(content, params={})
    content.gsub(/(^`{3,}\s*(\S*)\s*$([^`]*)^`{3,}\s*$)+?/m) {|match|
      lang_spec  = $2
      code_block = $3

      replacement = ''

      replacement << '<pre class="highlight"><code class="language'

      if lang_spec && lang_spec.length > 0
        replacement << '-'
        replacement << lang_spec
      end

      replacement << '">'

      code_block.gsub!("[:backtick:]", "`")

      coder = HTMLEntities.new
      replacement << coder.encode(code_block)
      replacement << '</code></pre>'
    }
  end
end
