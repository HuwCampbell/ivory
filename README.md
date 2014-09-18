ivory
=====

```
ivory: (the ivories) the keys of a piano
```

[![Build Status](https://travis-ci.org/ambiata/ivory.png)](https://travis-ci.org/ambiata/ivory)

Overview
--------

Ivory is a scalable and extensible data store for storing __facts__ and extracting __features__. It can 
be used within a large machine learning pipeline for normalising data and providing feeds to model
training and scoring pipelines.

Some interesting properties of Ivory are it:

* Has no moving parts - just files on disk;
* Is optimised for _scans_ not random access;
* Is extensible along the dimension of features;
* Is scalable by using HDFS or S3 as a backing store;
* Is an immutable data store allowing version "roll backs".


Concepts
--------

An Ivory __repository__ stores __facts__. A fact is comprised of 4 components: __entity__,
__attribute__, __value__, and __time__. That is, a *fact* represents the *value* of an
*attribute* associated with an *entity*, which is known to be valid at some point in *time*.
Examples of facts are:

 Entity     | Attribute   | Value     | Time
 -----------|-------------|-----------|---------------
 cust_00678 | gender      | M         | 2011-03-17
 acnt_1234  | balance     | 342.17    | 2014-06-01
 car_98732  | make        | Toyota    | 2012-09-25


Whilst there is no technical limitation, a given Ivory repository should only store facts
for a single class of entity. For example, you wouldn't store both "customer" and "account"
facts in the same Ivory repository.

The facts stored in an Ivory repository are *sparse*. That is, for each entity, there is no
requirement or expectation that a fact exists for all attributes or any fixed time intervals.
For example, a given attribute may only be present in a single fact associated with one entity
whilst other attributes may be present in facts associated with the majority or all entities.
Therefore, an Ivory repository is *extensible* along 3 dimensions: entity, attribute and time.

Facts are ingested into an Ivory repository in sets called __factsets__. A factset can include
facts for multiple entities, across multiple attributes, spanning any set of times. For example,
a factset for a *customer* Ivory repository might look like the following:

 Entity     | Attribute   | Value     | Time
 -----------|-------------|-----------|---------------
 cust_00678 | gender      | M         | 2011-03-17
 cust_00678 | zipcode     | 12345     | 2011-03-17
 cust_00435 | mthly_spend | 432.00    | 2014-05-01
 cust_00123 | gender      | F         | 2009-02-26
 cust_00123 | mthly_spend | 220.50    | 2014-05-01


For a factset to be ingested successfully, all referenced attributes must be declared in the
repository's __dictionary__. The dictionary is a declaration list of all known attributes
and metadata associated with them. For example:

 Namespace   | Name        | Encoding  | Description
 ------------|-------------|-----------|--------------------------------------
 demographic | gender      | string    | The customer's gender
 demographic | zipcode     | string    | The customer's zipcode
 account     | mthly_spend | double    | The customer's account spend in the last month
 
Note that an attribute is identified by a *name* and *namespace*. Namespaces are used as
a data partitioning mechanism internally. As a general rule of thumb, attributes that are
related should be contained in the same namespace. Similarly, unrelated attributes should
be contained in separate namespaces.

Also note that the source-of-truth for a dictionary is not the Ivory repository itself. The
dictionary is typically maintained in a text file (under version control) or a database, and
is periodically *imported* into an Ivory repository.

Facts in Ivory are intended to be queried in very particular ways. Specifically, the intention
is to extract per-entity __feature vectors__. Two types of extractions can be performed:

1. __Snapshots__: extract the latest values for attributes across entities with respect to
a given point in time;
2. __Chords__: extract the latest values for attributes across entities with respect to given
points in time for each entity.

At a high-level, chord extractions are typically performed when preparing feature vectors for
model training. Snapshot extractions are typically performed when preparing feature vectors for
model scoring.

Finally, an Ivory repository is a versioned immutable data store. Any time a repository is altered
(i.e. ingesting a factset or importing a dictionary), a new __version__ of the repository
is created. This allows extractions to be repeatedly performed against specific versions of a
repository without being affected by further repository updates.


Installing
----------

### Ivory

Ivory can be installed by running the following commands:

```
$ curl -fsSL https://raw.githubusercontent.com/ambiata/ivory/master/bin/install > install
$ chmod a+x install 
$ ./install /ivory/install/path
```

Once installed, add `/ivory/install/path/bin` to the `$PATH` environment variable. You can then
run:

```
$ ivory --help
```

### Dependencies

The `ivory` command requires the `hadoop` launch script to be on the path. If you don't already
have Hadoop installed, you can download a distribution such as
[CDH5](http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.2.0-cdh5.0.0-beta-2.tar.gz).


### Settings

Internally Ivory uses Snappy compression. Because it can be sometimes difficult to get the Snappy
native libraries install on OS X. By setting `export IVORY_NO_CODEC=true`, the use of Snappy
compression can be disabled. Note, this should not be set when running Ivory in production.


Further Documentation
---------------------

### User documentation

  * [API Compatibility](https://github.com/ambiata/ivory/blob/master/doc/user/api-compatability.md)
  * [Introductory presentation](https://speakerdeck.com/ambiata/ivory-an-introduction)


### Design documentation

  * [Design notes](https://github.com/ambiata/ivory/tree/master/doc/design)


Contributing and Issues
-----------------------

All bugs and feature requests can be raised as [GitHub issues](https://github.com/ambiata/ivory/issues).

All contributions are via [GitHub pull requests](https://help.github.com/articles/using-pull-requests). In general:
 - Try to provide enough detail as to what you are adding / fixing.
 - Add tests for any new features / bug fixes.
 - Make sure you are up-to-date with the latest commit on master.
 - If you are not sure about any process or change, you can ask on
   the [mailing list](https://groups.google.com/forum/#!forum/ivory-project) first.


Contact
-------

General ivory questions can be sent to the mailing list <https://groups.google.com/forum/#!forum/ivory-project>.

Ivory is developed by the engineering team at [Ambiata](http://ambiata.com/), feel free to get in contact with us:

> Ben Lever
> <ben.lever@ambiata.com>
> [@bmlever](https://twitter.com/bmlever)

> Mark Hibberd
> <mark.hibberd@ambiata.com>
> [@markhibberd](https://twitter.com/markhibberd)

> Russell Aronson
> <russell.aronson@ambiata.com>

> Eric Torreborre
> <eric.torreborre@ambiata.com>

> Max Swadling
> <maxwell.swadling@ambiata.com>

> Charles O'Farrell
> <charles.ofarrell@ambiata.com>
> [@charlesofarrell](https://twitter.com/charlesofarrell)
