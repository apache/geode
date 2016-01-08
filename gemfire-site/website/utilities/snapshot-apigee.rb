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

require 'rubygems'
require 'mechanize'
require 'anemone'
require 'pandoc-ruby'
# require 'json/ext'

TO_REMOVE = [ 'div.toc-filter-back-to-top',
              '.rate-yesno-title',
              'colgroup',
              'div.rate-widget',
              'div.toc-filter.toc-filter-bullet'  ]

puts "Crawling..."

urls = []
Anemone.crawl("http://apigee.com/docs/app_services", :skip_query_strings => true) do |anemone|
  # anemone.on_every_page {|page| puts page.url}
  # anemone.skip_links_like(/https?\:\/\/apigee.com\/docs\/(comment|node|api-platform|console|ja|enterprise|consoletogo)/)
  anemone.focus_crawl { |page| page.links.select{|l| l.to_s.match(/https?\:\/\/apigee.com\/docs\/(app-services|geode)\/content/) } }
  anemone.on_pages_like(/https?\:\/\/apigee.com\/docs\/(app-services|geode)\/content/) do |page|
    urls.push page.url
    # puts "Found #{page.url}"
  end
  # anemone.after_crawl {  }
end

urls = urls.compact.map{|u| u.to_s}.uniq.sort

puts "Found #{urls.size} documentation articles"
puts urls.join("\n")
gets

a = Mechanize.new { |agent|
  agent.user_agent_alias = 'Mac Safari'
}

urls.each do |url|
  name = url.split('/')[-1]
  puts "Processing #{name}"
  begin
    a.get(url) do |article|
      # title = article.search('h1').first
      body = article.search('section#block-system-main>div.node>div.field-name-body').first
      next if body.nil?
      # body.children.first.add_previous_sibling(title)
      # body.search('br').each {|l| l.remove}
      body.search(TO_REMOVE.join(', ')).each {|l| l.remove}
      body.search('div#collapse').each do |div|
        div.add_next_sibling '<a id="'+div.attributes['id'].value+'"></a>'
        div.remove
      end
      body.search('h2').each {|h| h.remove_attribute('class')}
      body.search('*').each{|n| n.remove_attribute('style')}
      body.search("a").each do |link|
        begin
          link.attributes["href"].value = link.attributes["href"].value.gsub(/^\/docs\/app-services\/content\//,'/')
        rescue
        end
      end
      markdown = PandocRuby.convert(body, :from => :html, :to => :markdown)
      front_matter = "---\ntitle: #{title.inner_html.gsub(':',' - ')}\ncategory: \nlayout: article\n---\n\n"
      markdown.gsub!('Apigee App Services', 'Apache Usergrid')
      markdown.gsub!('App Services', 'Apache Usergrid')
      markdown.insert(0,front_matter)
      today = Time.new.strftime('%Y-%m-%d')
      File.open("../content/docs/#{today}-#{name}.md", 'w') {|f| f.write(markdown) }
    end
  rescue Exception => e
    puts e
  end
end
