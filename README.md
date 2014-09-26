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
> curl -OfsSL https://raw.githubusercontent.com/ambiata/ivory/master/bin/install
> chmod a+x install
> ./install /ivory/install/path
```

Once installed, add `/ivory/install/path/bin` to the `$PATH` environment variable. You can then
run:

```
> ivory --help
```

### Dependencies

The `ivory` command requires the `hadoop` launch script to be on the path. If you don't already
have Hadoop installed, you can download a distribution such as
[CDH5](http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.2.0-cdh5.0.0-beta-2.tar.gz).


### Settings

Internally Ivory uses Snappy compression. Because it can be sometimes difficult to get the Snappy
native libraries install on OS X. By setting `export IVORY_NO_CODEC=true`, the use of Snappy
compression can be disabled. Note, this should not be set when running Ivory in production.


An example
----------

In this example we will be creating an Ivory repository for the customers of a fictious
online payment provider called __HipPay__. Using the `ivory` command line tool, we will
create and interact with an Ivory repository. First we can create a new Ivory repository:

```
# Used by most ivory commands instead of setting --repository each time
> export IVORY_REPOSITORY=hippay
> ivory create-repository -z Australia/Sydney $IVORY_REPOSITORY
```

Before we can begin ingesting factsets, we first need to create a dictionary and
import it into the repository:

```
> cat dictionary.psv
demographic:gender|encoding=string|description=The customers's gender
demographic:age|encoding=int|description=The customer's age in years
demographic:state_of_residence|encoding=string|description=The state of the customer's residential address
account:type|encoding=string|description=The type of the cutomer's account
account:balance|encoding=double|description=The customer's account balance
payment:total_outgoing_1m|encoding=int|description=Number of outgoing payments in the past 1 month
payment:total_incoming_1m|encoding=int|description=Number of incoming payments in the past 1 month

> ivory import-dictionary --path hippay_dict.psv
```

We can view the repository's dictionary at any point in time using the `cat-dictionary`
command:

```
> ivory cat-dictionary
payment:total_incoming_1m|encoding=int|description=Number of incoming payments in the past 1 month
payment:total_outgoing_1m|encoding=int|description=Number of outgoing payments in the past 1 month
account:balance|encoding=double|description=The customer's account balance
account:type|encoding=string|description=The type of the cutomer's account
demographic:state_of_residence|encoding=string|description=The state of the customer's residential address
demographic:age|encoding=int|description=The customer's age in years
demographic:gender|encoding=string|description=The customers's gender
```

When signing up, HipPay customers have the option of specifying their gender and age. These
can be used as facts and ingested into the Ivory repository:

```
> cat factset1.psv
mike|gender|M|2013-04-26
jill|gender|F|2012-07-19
jill|age|25|2012-07-19
fred|gender|M|2014-02-11
fred|age|37|2014-02-11
mary|age|42|2013-11-24

> ivory ingest --input factset1.psv --namespace demographic -z "Australia/Sydney"
```

Note that the facts do not specify attribute namespaces. In this factset, because all facts
are for attributes in the `demographic` namespace, we simply specify that as a command line
argument.

Having ingested a factset into the repository, we can run our first snapshot extract
for the arbitrary date, 2014-06-01:

```
> ivory snapshot --missing-value 'NULL' --date 2014-06-01 --output dense:csv=snapshot_20140601

> cat snapshot_20140601/out*
fred,NULL,NULL,37,M,NULL,NULL,NULL
jill,NULL,NULL,25,F,NULL,NULL,NULL
mary,NULL,NULL,42,NULL,NULL,NULL,NULL
mike,NULL,NULL,NULL,M,NULL,NULL,NULL

> cat snapshot_20140601/.dictionary
0,account|balance|double||The customer's account balance|NULL
1,account|type|string||The type of the cutomer's account|NULL
2,demographic|age|int||The customer's age in years|NULL
3,demographic|gender|string||The customers's gender|NULL
4,demographic|state_of_residence|string||The state of the customer's residential address|NULL
5,payment|total_incoming_1m|int||Number of incoming payments in the past 1 month|NULL
6,payment|total_outgoing_1m|int||Number of outgoing payments in the past 1 month|NULL
```

A snapshot will produce feature vectors for each of the entities in the repository. It is accompanied
by a "dictionary" file that specifies the feature vector column ordering.

We can of course extract a snapshot at a different date as well, for example, 2014-01-01:

```
> ivory snapshot --missing-value 'NULL' --date 2014-01-01 --output dense:csv=snapshot_20140101

> cat snapshot_20140101/out*
jill,NULL,NULL,25,F,NULL,NULL,NULL
mary,NULL,NULL,42,NULL,NULL,NULL,NULL
mike,NULL,NULL,NULL,M,NULL,NULL,NULL
```

Note that in this case, becasue `fred` has no facts prior to 2014-01-01, there is no feature vector
for him in the extract.

At any point in time we can ingest additional factsets:

```
> cat factset2.psv
mike|type|BASIC|2013-04-26
jill|type|BASIC|2012-07-19
fred|type|XTREME|2014-02-11
mary|type|STANDARD|2013-11-24

> ivory ingest --input factset2.psv -z "Australia/Sydney" --namespace account
```

Now if we extract a snapshot at 2014-06-01 again, we can see that the `type` column is now populated:

```
> ivory snapshot --missing-value 'NULL' --date 2014-06-01 --output dense:csv=snapshot_20140601.2

> cat snapshot_20140601.2/out*
fred,NULL,XTREME,37,M,NULL,NULL,NULL
jill,NULL,BASIC,25,F,NULL,NULL,NULL
mary,NULL,STANDARD,42,NULL,NULL,NULL,NULL
mike,NULL,BASIC,NULL,M,NULL,NULL,NULL
```

In the above factset you will notice a `type` value of `XTREME`. It turns out that this is
a data generation bug and should actually be `EXTREME`. We can fix this fact by ingesting
a new factset with a corrected fact. If we then extract the snapshot again, we will see that
the value has been corrected:

```
> cat factset3.psv
fred|type|EXTREME|2014-02-11

> ivory ingest --input factset3.psv -z "Australia/Sydney" --namespace account

> ivory snapshot --missing-value 'NULL' --date 2014-06-01 --output dense:csv=snapshot_20140601.3

> cat snapshot_20140601.3/out*
fred,NULL,EXTREME,37,M,NULL,NULL,NULL
jill,NULL,BASIC,25,F,NULL,NULL,NULL
mary,NULL,STANDARD,42,NULL,NULL,NULL,NULL
mike,NULL,BASIC,NULL,M,NULL,NULL,NULL
```


Coming soon
-----------

* Transparent interoperability between different filesystems (HDFS, S3, local)
* Improved dictionary import formats
* Virtual features
* Repository forking
* Feature selection
* Improved fact validation on ingest


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

> Charles O'Farrell
> <charles.ofarrell@ambiata.com>
> [@charlesofarrell](https://twitter.com/charlesofarrell)

> Nick Hibberd
> <nick.hibberd@ambiata.com>

You can also follow the project on Twitter [@ivoryproject](https://twitter.com/ivoryproject).
