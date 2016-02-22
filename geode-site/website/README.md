# Apache Geode Website

This directory contains the source files for the project website. Website content is written in [Markdown](https://help.github.com/articles/markdown-basics) and the site files are generated from that source by a tool called [Pandoc](http://johnmacfarlane.net/pandoc).

Source files for the website are in ``${geode-project-dir}/geode-site/website/content``

Generated files for the website are in ``${geode-project-dir}/geode-site/content``

The website is updated by a "sync" tool that monitors the __asf-site__ branch of our Git repo, so after to make changes you must push your updated source and generated files to that branch. The content will be published to the [Geode website](http://geode.incubator.apache.org), after a 5-20 minute delay.

## Prerequsites

To generate the site locally, you need Ruby, Python, Pandoc and a couple of Ruby Gems.

Install Pandoc (Haskell-based markup format converter):

    http://johnmacfarlane.net/pandoc/installing.html

Install Pygments (Python-based syntax coloring library):

    $ sudo easy_install Pygments

Install Nanoc and other Ruby Gems needed:

    $ sudo gem install nanoc -v 3.8.0
    $ sudo gem install pygments.rb htmlentities pandoc-ruby nokogiri rack mime-types

## How to change/update the website

### 1. Find and edit the source files you need to change

Generally, you should make your changes in the __master__ branch unless you have a very good reason to do otherwise. When you're ready to publish, merge them to the __asf-site__ branch.

If you are changing the actual content of the site, then find Markdown file that you need to edit under the ``content/docs`` directory and make your change.

If you need to change the layout or styling of the site, then you will probably need to change an HTML, JS or CSS file under the ``content`` directory.

## 2. Test your changes locally

To test locally, you can use the autocompiler (will build changes on every request) and check the website at [http://0.0.0.0:3000](http://0.0.0.0:3000)

    $ nanoc autocompile

## 3. Publish your changes to the site    

Run the nanoc compiler to generate the site. It is configured via the ``nanoc.yaml`` to place website files into the ``content`` directory at the top

    $ nanoc compile

Run ``git status`` and you should see your changes plus some update files under the ``${geode-project-dir}/content`` directory.

Once you are happy with your changes, commit them, merge to the __asf-site__ branch and push.

The site should update in 5-10 minutes and if not [file a JIRA against the INFRA project](https://issues.apache.org/jira/browse/INFRA) or ask for advice on the Infrastructure project's HipChat room [#asfinfra](https://www.hipchat.com/g4P84gemn).
