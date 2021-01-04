Herringbone
===========

> _**Herringbone is deprecated and is no longer being actively maintained.**_

Herringbone is a suite of tools for working with parquet files on hdfs, and with impala and hive.

The available commands are:

`flatten`: transform a directory of parquet files with a nested structure into a directory of parquet files with a flat schema that can be loaded into impala or hive (neither of which support nested schemas). Default output directory is `/path/to/input/directory-flat`.

    $ herringbone flatten -i /path/to/input/directory [-o /path/to/non/default/output/directory]

`load`: load a directory of parquet files (which must have a flat schema) into impala or hive (defaulting to impala). Use the --nocompute-stats option for faster loading into impala (but probably slower querying later on!)

    $ herringbone load [--hive] [-u] [--nocompute-stats] -d db_name -t table -p /path/to/parquet/directory

`tsv`: transform a directory of parquet files into a directory of tsv files (which you can concat properly later with `hadoop fs -getmerge /path/to/tsvs`). Default output directory is `/path/to/input/directory-tsv`.

    $ herringbone tsv -i /path/to/input/directory [-o /path/to/non/default/output/directory]

`compact`: transform a directory of parquet files into a directory of fewer larger parquet files. Default output directory is `/path/to/input/directory-compact`.

    $ herringbone compact -i /path/to/input/directory [-o /path/to/non/default/output/directory]

See `herringbone COMMAND --help` for more information on a specific command.

Building
--------

You'll need thrift 0.9.1 on your path.

    $ git clone github.com/stripe/herringbone
    $ cd herringbone
    $ mvn package

Authors
-------

 - [Avi Bryant](http://twitter.com/avibryant)
 - [Danielle Sucher](http://twitter.com/daniellesucher)
 - [Jeff Balogh](http://twitter.com/jbalogh)
