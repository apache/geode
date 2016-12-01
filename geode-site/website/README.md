# Apache Geode Website

This directory contains the source files for the project website. Website content is written in [Markdown](https://help.github.com/articles/markdown-basics) and the site files are generated from that source by a tool called [Pandoc](http://johnmacfarlane.net/pandoc).

Source files for the website are in `${geode-project-dir}/geode-site/website/content`.

Generated files for the website are in `${geode-project-dir}/geode-site/content`.

NOTE: To make changes to the [Apache Geode User Guide](http://geode.apache.org/docs/guide/About_Geode.html):

- See `${geode-project-dir}/geode-docs/CONTRIBUTE.md` for information about contributing to the documentation source files.
- See `${geode-project-dir}/geode-book/README.md` for information about building a local version of the guide and adding it to the website.

The website is updated by a "sync" tool that monitors the __asf-site__ branch 
of our Git repo, so after making changes you must place your updated source
and generated files on the __asf-site__ branch and push.
The content will be published to the
Geode website](http://geode.apache.org) after a 5-20 minute delay.

## Prerequisites

To generate the site locally, you need Ruby, Python, Pandoc and a couple of Ruby Gems.

Install Pandoc (Haskell-based markup format converter):

    http://johnmacfarlane.net/pandoc/installing.html

Install Pygments (Python-based syntax coloring library):

    $ sudo easy_install Pygments

Install Nanoc and other Ruby Gems needed:

    $ sudo gem install nanoc -v 4.2.0
    $ sudo gem install pygments.rb htmlentities pandoc-ruby nokogiri rack mime-types adsf

## How to change/update the website

### 1. Find and edit the source files you need to change

Source files for the website are in
``${geode-project-dir}/geode-site/website/content``.
When changing the actual content of the site, find the Markdown files that you
need to edit under the `${geode-project-dir}/geode-site/website/content/`
directory and make your change.

If you need to change the layout or styling of the site,
then you will probably need to change an HTML, JS or CSS file 
within the ``${geode-project-dir}/geode-site/website/content`` directory.

NOTE: The [Apache Geode User Guide](http://geode.apache.org/docs/guide/About_Geode.html) source files are in `${geode-project-dir}/geode-docs/`. See `${geode-project-dir}/geode-docs/CONTRIBUTE.md` for more information.

### 2. Locally generate the site and test your changes

Run the nanoc compiler to generate the site.
Nanoc is configured by the
``${geode-project-dir}/geode-site/website/nanoc.yaml``
file to place the locally built website into the
``${geode-project-dir}/geode-site/content`` directory.
With a cwd of ``${geode-project-dir}/geode-site/website``:

    $ nanoc compile
Run ``git status`` and you should see your changes plus any updated files
under the ``${geode-project-dir}/content`` directory.

NOTE: Whether or not you have made changes to the Apache Geode User Guide, you must now build the User Guide and move the User Guide files to the Geode website. For instructions, see `${geode-project-dir}/geode-book/README.md`.

To view your changes locally, use the view command to start a local web server. Check the website at [http://0.0.0.0:3000](http://0.0.0.0:3000)

    $ nanoc view

To make further changes, stop the web server, edit files, recompile, and view again.

### 3. Publish your changes to the site    

Once you are happy with your changes, commit them to the __develop__ branch.
The changes also need to be propagated to the __asf-site__ branch.
However, the file structure of the __asf-site__ branch is unusual, so a
git merge will not do the right thing.

The compiled ``${geode-project-dir}/geode-site/content`` directory 
from the __develop__ branch will need to be placed at the 
top level, ``${geode-project-dir}``, of the __asf-site__ branch.
Here is one way to accomplish this:

1. On the __develop__ branch

        $ cd geode-site/website
        $ nanoc compile
        $ cd ../content
        $ tar cvf new-website-content.tar .
        $ mv new-website-content.tar ~/Desktop/

2. Expand the TAR file at the top level of the __asf-site__ branch (cwd should be ${geode-project-dir}:

        $ git checkout asf-site
        $ tar xvf ~/Desktop/new-website-content.tar

3. Commit and push on the __asf-site__ branch

The site should update in 5-10 minutes. If it does not, [file a JIRA against the INFRA project](https://issues.apache.org/jira/browse/INFRA) or ask for advice on the Infrastructure project's HipChat room [#asfinfra](https://www.hipchat.com/g4P84gemn).
