package com.ambiata.ivory.mr

import com.ambiata.ivory.core.IvoryVersion
import com.ambiata.poacher.mr.MrContext
import org.apache.hadoop.mapreduce.Job

object MrContextIvory {

  def newContext(namespace: String, job: Job): MrContext = {
    job.getConfiguration.set("ivory.version", IvoryVersion.get.version)
    MrContext.newContext(namespace, job)
  }
}
