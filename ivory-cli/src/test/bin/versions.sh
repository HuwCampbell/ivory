#!/bin/sh -eu

######################
### Ivory Versions ###
######################

# Baseline version
export IVORY_V0="1.0.0-cdh5-20140713234709-4656823 2.10"

# Dictionary identifier names
# Dictionary thrift format
export IVORY_V1="1.0.0-cdh5-20140709064501-62fc154 2.10"

# Structs
# Thrift ingest
export IVORY_V2="1.0.0-cdh5-20140805003023-b503696 2.11"

# JSON snapshot metadata
# New ingest input format
export IVORY_V3="1.0.0-cdh5-20141217212706-02d69f4 2.11"

# Last version before v2 metadata where size was added to factset metadata.
export IVORY_V4="1.0.0-cdh5-20141229233818-6d996f7 2.11"

# Last version before v3 metadata where old snapshots manifests were actually written out.
export IVORY_V5="1.0.0-cdh5-20150112052717-f20b962 2.11"

# The _first_ version that had the current metadata version - see 'dictionary-metadata' test
export IVORY_BASELINE="1.0.0-cdh5-20150310031300-d62a1e9 2.11"
