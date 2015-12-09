# All files in the 'lib' directory will be loaded
# before nanoc starts compiling.
include Nanoc::Helpers::Rendering

require 'pandoc-ruby'
require 'htmlentities'

class PandocFilter < Nanoc3::Filter
  identifier :pandoc
  type :text

  def run(content, params = {})
    ::PandocRuby.convert(content, 'smart', 'no-highlight', 'toc', :template => 'lib/pandoc.template')
  end
end

class FencedCodeBlock < Nanoc3::Filter
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