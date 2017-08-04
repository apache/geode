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

require 'bookbinder/code_example_reader'
require 'bookbinder/ingest/cloner_factory'
require 'bookbinder/ingest/git_accessor'
require 'bookbinder/local_filesystem_accessor'
require 'date'
require_relative 'archive_drop_down_menu'
require_relative 'quicklinks_renderer'

I18n.enforce_available_locales = false

module Bookbinder
  class Helpers < ::Middleman::Extension
    # class << self
    #   def registered(app)
    #     app.helpers HelperMethods
    #   end

    #   alias :included :registered
    # end

    module HelperMethods

      def yield_for_code_snippet(from: nil, at: nil)
        cloner_factory = Ingest::ClonerFactory.new({out: $stdout},
                                                   LocalFilesystemAccessor.new,
                                                   Ingest::GitAccessor.new)

        cloner = cloner_factory.produce(config[:local_repo_dir])
        code_example_reader = CodeExampleReader.new({out: $stdout},
                                                    LocalFilesystemAccessor.new)
        working_copy = cloner.call(source_repo_name: from,
                                   source_ref: 'master',
                                   destination_parent_dir: config[:workspace])

        snippet, language = code_example_reader.get_snippet_and_language_at(at, working_copy)

        delimiter = '```'

        snippet.prepend("#{delimiter}#{language}\n").concat("\n#{delimiter}")
      end

      def elastic_search?
        !!config[:elastic_search]
      end

      def yield_for_subnav
        partial "subnavs/#{subnav_template_name}"
      end

      def yield_for_archive_drop_down_menu
        menu = ArchiveDropDownMenu.new(
          config[:archive_menu],
          current_path: current_page.path
        )
        unless menu.empty?
          partial 'archive_menus/default', locals: { menu_title: menu.title,
                                                     dropdown_links: menu.dropdown_links }
        end
      end

      def exclude_feedback
        current_page.add_metadata({page: {feedback_disabled: true}})
      end

      def yield_for_feedback
        partial 'layouts/feedback' if config[:feedback_enabled] && !current_page.metadata[:page][:feedback_disabled]
      end

      def exclude_repo_link
        current_page.add_metadata({page: {repo_link_disabled: true}})
      end

      def render_repo_link
        if config[:repo_link_enabled] && repo_url && !current_page.metadata[:page][:repo_link_disabled]
          "<a id='repo-link' href='#{repo_url}'>View the source for this page in GitHub</a>"
        end
      end

      def mermaid_diagram(&blk)
        escaped_text = capture(&blk).gsub('-','\-')

        @_out_buf.concat "<div class='mermaid'>#{escaped_text}</div>"
      end

      def modified_date(default_date: nil)
        parsed_default_date = Time.parse(default_date).utc if default_date

        date = page_last_modified_date || parsed_default_date

        "Page last updated: <span data-behavior=\"DisplayModifiedDate\" data-modified-date=\"#{date.to_i}000\"></span>" if date
      end

      def breadcrumbs
        page_chain = add_ancestors_of(current_page, [])
        breadcrumbs = page_chain.map do |page|
          make_breadcrumb(page, page == current_page)
        end.compact
        return if breadcrumbs.size < 2
        return content_tag :ul, breadcrumbs.reverse.join(' '), class: 'breadcrumbs'
      end

      def vars
        OpenStruct.new config[:template_variables]
      end

      ## Geode helpers (start)
      def geode_product_name
        current_page.data.title= vars.geode_product_name
      end

      def geode_product_name_long
        current_page.data.title= vars.geode_product_name_long
      end

      def geode_product_version
        current_page.data.title= vars.geode_product_version
      end

      def set_title(*args)
        current_page.data.title= args.join(' ')
      end
      ## Geode helpers (end)

      def product_info
        config[:product_info].fetch(template_key, {})
      end

      def production_host
        config[:production_host]
      end

      def quick_links
        page_src = File.read(current_page.source_file)
        quicklinks_renderer = QuicklinksRenderer.new(vars)
        Redcarpet::Markdown.new(quicklinks_renderer).render(page_src)
      end

      def owners
        html_resources = sitemap.resources.select { |r| r.path.end_with?('.html') }
        html_resources.each.with_object({}) { |resource, owners|
          owners[resource.path] = Array(resource.data['owner'])
        }
      end

      def template_key
        decreasingly_specific_namespaces.detect { |ns|
          config[:subnav_templates].has_key?(ns)
        }
      end

      def body_classes(path=current_path.dup, options={})
        if path.is_a? Hash
          options = path
          path = current_path.dup
        end
        basename = File.basename(path)
        dirname = File.dirname(path).gsub('.', '_')
        page_classes(File.join(dirname, basename), options)
      end

      private

      def subnav_template_name
        config[:subnav_templates][template_key] || 'default'
      end

      def decreasingly_specific_namespaces
        body_classes(numeric_prefix: numeric_class_prefix).
          split(' ').reverse.drop(1).
          map {|ns| ns.sub(/^#{numeric_class_prefix}/, '')}
      end

      def numeric_class_prefix
        'NUMERIC_CLASS_PREFIX'
      end

      def page_last_modified_date
        git_accessor = Ingest::GitAccessor.new

        current_date = if current_page.data.dita
          git_accessor.author_date(preprocessing_path(current_page.source_file), dita: true)
        else
          git_accessor.author_date(current_page.source_file)
        end

        current_date.utc if current_date
      end

      def repo_url
        nested_dir, filename = parse_out_nested_dir_and_filename

        repo_dir = match_repo_dir(nested_dir)
        page_repo_config = config[:repo_links][repo_dir]

        if page_repo_config && page_repo_config['ref']
          org_repo = Pathname(page_repo_config['repo'])
          ref = Pathname(page_repo_config['ref'])
          at_path = at_path(page_repo_config)
          nested_dir = extract_nested_directory(nested_dir, repo_dir)

          "http://github.com/#{org_repo.join(Pathname('tree'), ref, Pathname(nested_dir), at_path, source_file(filename))}"
        end
      end

      def match_repo_dir(nested_dir)
        config[:repo_links].keys
          .select{ |key| nested_dir.match(/^#{key}/) }
          .sort_by{ |key| key.length }
          .last
      end

      def source_file(filename)
        fs = LocalFilesystemAccessor.new

        if current_page.data.dita
          source_filename = "#{filename}.xml"

          if fs.source_file_exists?(Pathname(preprocessing_path(current_page.source_file)).dirname,
                                             source_filename)
            source_filename
          else
            ''
          end
        else
          "#{filename}.html.md.erb"
        end
      end

      def preprocessing_path(current_source_path)
        root_path, nested_repo_path = current_source_path.split('source')

        root_path.gsub!('/output/master_middleman', '')

        "#{root_path}output/preprocessing/sections#{nested_repo_path}"
      end

      def parse_out_nested_dir_and_filename
        current_page.path
          .match(/\/?(.*?)\/?([^\/]*)\.html$?/)
          .captures
      end

      def extract_nested_directory(nested_dir, repo_dir)
        nested_dir = nested_dir.gsub("#{repo_dir}", '')
        nested_dir = nested_dir.sub('/', '') if nested_dir[0] == '/'

        nested_dir
      end

      def at_path(page_repo_config)
        path = page_repo_config['at_path'] || ""

        Pathname(path)
      end

      def add_ancestors_of(page, ancestors)
        if page
          add_ancestors_of(page.parent, ancestors + [page])
        else
          ancestors
        end
      end

      def make_breadcrumb(page, is_current_page)
        return nil unless (text = page.data.breadcrumb || page.data.title)
        if is_current_page
          css_class = 'active'
          link = content_tag :span, text
        else
          link = link_to(text, '/' + page.path)
        end
        content_tag :li, link, :class => css_class
      end
    end
    
    helpers HelperMethods
    

  end
end
::Middleman::Extensions.register(:bookbinder, Bookbinder::Helpers)
