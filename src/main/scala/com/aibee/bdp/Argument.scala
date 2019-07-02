// @Time    : 2019-06-28 20:24
// @Author  : WuTong
// @Email   : twu@aibee.com
// @FileName: Argument.py
// @Software: IntelliJ IDEA

package com.aibee.bdp

import java.text.SimpleDateFormat
import java.util.Calendar

import org.rogach.scallop._

class Argument(arguments: Seq[String]) extends ScallopConf(arguments) {
  val appName = opt[String](required = true)
  val topics = opt[String](required = true)
  val kafkaBootstrapServers = opt[String](required = true)
  val outputDir = opt[String](required = true)

  val lastOffsetPath = opt[String]()
  val offsetPath = opt[String](required = true)
  val backfill = opt[Boolean](default = Some(false))
  val dt = opt[String]()
  verify()

  def getDt: String = dt.getOrElse(new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance.getTime))
}
