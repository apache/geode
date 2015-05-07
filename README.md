### URL

[http://geode.incubator.apache.org](http://geode.incubator.apache.org)

### Changes

To add to or change the header, navigation or footer, use the _layouts/default.html file.

To add a new top-level page (e.g. /download), follow the existing format (folder using the name you want and then an index.md inside).  New pages will need to have the "YAML Front Matter" at the top (the information bracketed by triple dashes).

To add a second-level page (e.g. /community/secondarypage), add a .md file to the correct directory using the name you want and the correct YAML front matter at the top of the file.

### Technologies

+ GitHub Pages use [Jekyll](http://jekyllrb.com/)
+ .md files are Markdown ([Cheat sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet))  .md can also support HTML.
+ .scss allows both SASS and CSS ([SASS cheat sheet](http://sass-cheatsheet.brunoscopelliti.com/))

### Run locally

Ignore the _site directory.  This directory is rebuilt every time Jekyll rebuilds the site and will not be committed to GitHub.

```
git clone git@github.com:apache/incubator-geode.git
cd docs
git checkout asf-site
gem install jekyll
jekyll serve
```


