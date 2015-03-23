---
title: Install Ivory on Mac OS X
---

These instructions have been tested on Yosemite 10.10.2.

## Java

Java™ must be installed. Check your version of Java by running:

    java -version

Recommended Java versions for Hadoop are listed at [http://wiki.apache.org/hadoop/HadoopJavaVersions](http://wiki.apache.org/hadoop/HadoopJavaVersions).

If you don’t have a recommended version of Java, download one from  [http://www.oracle.com/technetwork/java/javase/downloads/index.html](http://www.oracle.com/technetwork/java/javase/downloads/index.html), and begin installing Java by double clicking the downloaded `.dmg` file.

Set `JAVA_HOME` by adding the following lines to `~/.profile` (for bash-style shells):

    export JAVA_HOME=Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/bin
    export PATH=$PATH:$JAVA_HOME

Now run:

    $ source ~/.profile

Check `JAVA_HOME` by running:

    $ echo $JAVA_HOME

You should see something like:

    java version “1.7.0_45"

## SSH

`ssh` must be installed and `sshd` must be running to use the Hadoop scripts that manage remote Hadoop daemons.

Check if `sshd` is running:

    $ ps -A | grep ssh

You should see something like:

    72205 ??         0:00.41 sshd: username@ttys003

If not, go to _System Preferences_ > _Sharing_ and check the _Remote Login_ option.

## Hadoop

Download Hadoop from [http://www.apache.org/dyn/closer.cgi/hadoop/common/](http://www.apache.org/dyn/closer.cgi/hadoop/common/).

Unpack the downloaded Hadoop distribution to `usr/local/hadoop`. In the distribution, edit  `etc/hadoop/hadoop-env.sh` to add:

    # Assuming your installation directory is /usr/local/hadoop
    export HADOOP_PREFIX=/usr/local/hadoop

Note that Hadoop requires Java, but we've already defined `JAVA_HOME`.

Check the Hadoop installation by running:

    $ hadoop version

You should see something like:

    Hadoop 2.6.0

Now set HADOOP_HOME (so Ivory can find Hadoop) by adding the following lines to `~/.profile`:

    HADOOP_HOME=/usr/local/hadoop
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

Then run:

    $ source ~/.profile

Check HADOOP_HOME by running:

    $ echo $HADOOP_HOME

You should see something like:

    /usr/local/hadoop

## Ivory

Install Ivory by rumming:

    $ curl -OfsSL https://raw.githubusercontent.com/ambiata/ivory/master/bin/install
    $ chmod a+x install
    $ ./install /usr/local/ivory

If `wget` isn’t on your system you'll see:

    ./install: line 10: wget: command not found

Install `wget` by following the instructions at [http://coolestguidesontheplanet.com/install-and-configure-wget-on-os-x/](http://coolestguidesontheplanet.com/install-and-configure-wget-on-os-x/).

Add Ivory to the PATH by adding the following lines to `~/.profile`:

    # ivory home
    export PATH=$PATH:/usr/local/ivory/bin

and then run:

    $ source ~/.profile

Now run Ivory:

    $ ivory —-help

You should see:

    Ivory 1.0.0-cdh5-20141113112607-b49d4ba
    Usage: {rename|cat-dictionary|cat-errors|cat-facts|chord|config|convert-dictionary|count-facts|create-repository|debug-dump-facts|debug-dump-reduction|fact-diff|import-dictionary|ingest|recompress|recreate|snapshot|factset-statistics}
