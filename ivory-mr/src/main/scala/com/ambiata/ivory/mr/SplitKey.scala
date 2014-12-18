package com.ambiata.ivory.mr

import org.apache.hadoop.mapreduce.InputSplit

/* An unsafe nugget of a data type, that lets us pass back the split with each
   key value.

     NOTE: designed to be updated in place by record reader.  */
class SplitKey[A](var split: InputSplit, var value: A)
