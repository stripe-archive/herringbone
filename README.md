Herringbone
===========

Herringbone is a suite of tools for working with parquet files on hdfs, and with impala and hive.

The available commands are:

`flatten`: transform a directory of parquet files with a nested structure into a directory of parquet files with a flat schema that can be loaded into impala or hive (neither of which support nested schemas)

    $ herringbone flatten -i /path/to/input/directory -o /path/to/output/directory

`load`: load a directory of parquet files (which must have a flat schema) into impala or hive (defaulting to impala)

    $ herringbone load [--hive] [-u] -d db_name -t table -p /path/to/parquet/directory

`tsv`: transform a directory of parquet files into a directory of tsv files (which you can concat properly later with `hadoop fs -getmerge /path/to/tsvs`)

    $ herringbone tsv -i /path/to/input/directory -o /path/to/output/directory

`compact`: transform a directory of parquet files into a directory of fewer larger parquet files

    $ herringbone compact -i /path/to/input/directory -o /path/to/output/directory

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
