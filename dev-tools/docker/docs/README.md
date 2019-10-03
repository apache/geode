[Apache Geode](http://github.com/apache/geode) User Guide is written in markdown format, which can be transformed to an HTML user guide using [Bookbinder](https://github.com/pivotal-cf/bookbinder).

`build-docs.sh` script allows to transform the Markdown files to HTML using Docker, without installing Bookbinder or Ruby.

After the HTML files are generated `view-docs.sh` can be used to start a webserver and review the documentation. The script makes the user guide available at `http://localhost:9292`.

The rest of files in this folder (`build-image-common.sh` and `Dockerfile`) are utilities used by `build-docs.sh` & `view-docs.sh`.
