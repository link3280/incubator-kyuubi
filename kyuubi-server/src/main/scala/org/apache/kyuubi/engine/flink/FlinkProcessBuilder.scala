/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink

import java.io.{File, FileOutputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder.TAG_KEY
import org.apache.kyuubi.operation.log.OperationLog

/**
 * A builder to build flink sql engine progress.
 */
class FlinkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  val flinkHome: String = getEngineHome(shortName)

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override def clusterManager(): Option[String] = Some("yarn")

  override protected val commands: Array[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new ArrayBuffer[String]()
    buffer += s"$flinkHome${File.separator}bin${File.separator}flink"
    buffer += "run-application"

    val javaOptions = conf.get(ENGINE_FLINK_JAVA_OPTIONS)
    if (javaOptions.isDefined) {
      buffer += javaOptions.get
    }

    // add session context kyuubi configurations
    conf.set(KYUUBI_SESSION_USER_KEY, proxyUser)

    // write kyuubi configuration
    val persistedConf = new Properties()
    val ioTmpDir = System.getProperty("java.io.tmpdir")
    val tmpKyuubiConf = s"$ioTmpDir${File.separator}$KYUUBI_CONF_FILE_NAME";
    persistedConf.putAll(conf.getAll.asJava)
    persistedConf.store(
      new FileOutputStream(tmpKyuubiConf),
      "persisted Kyuubi conf for Flink SQL engine")
    buffer += s"-Dyarn.ship-files=$tmpKyuubiConf"
    buffer += s"-Dyarn.tags=${conf.getOption(TAG_KEY).get}"
    buffer += "-Dcontainerized.master.env.FLINK_CONF_DIR=."
    buffer += "--class"
    buffer += s"$mainClass"

    buffer += s"${mainResource.get}"

    buffer.toArray
  }

  override def shortName: String = "flink"
}

object FlinkProcessBuilder {
  final val APP_KEY = "yarn.application.name"
  final val TAG_KEY = "yarn.tags"
  final val FLINK_HADOOP_CLASSPATH_KEY = "FLINK_HADOOP_CLASSPATH"
}
