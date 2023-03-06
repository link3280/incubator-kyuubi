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

import java.io.File

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder._

class FlinkProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")

  private def envDefault: ListMap[String, String] = ListMap(
    "JAVA_HOME" -> s"${File.separator}jdk",
    "FLINK_HOME" -> s"${File.separator}flink")
  private def envWithoutHadoopCLASSPATH: ListMap[String, String] = envDefault +
    ("HADOOP_CONF_DIR" -> s"${File.separator}hadoop${File.separator}conf") +
    ("YARN_CONF_DIR" -> s"${File.separator}yarn${File.separator}conf") +
    ("HBASE_CONF_DIR" -> s"${File.separator}hbase${File.separator}conf")
  private def envWithAllHadoop: ListMap[String, String] = envWithoutHadoopCLASSPATH +
    (FLINK_HADOOP_CLASSPATH_KEY -> s"${File.separator}hadoop")
  private def confStr: String = {
    conf.clone.set("yarn.tags", "KYUUBI").getAll
      .map { case (k, v) => s"\\\\\\n\\t--conf $k=$v" }
      .mkString(" ")
  }
  private def matchActualAndExpected(builder: FlinkProcessBuilder): Unit = {
    val actualCommands = builder.toString
    val expectedCommands =
      s"${envDefault("FLINK_HOME")}/bin/flink run-application " +
        s"-Dyarn.ship-files=./kyuubi-defaults.conf " +
        s"-Dcontainerized.master.env.FLINK_CONF_DIR=. " +
        s".*kyuubi-flink-sql-engine_.*jar"
    val regex = new Regex(expectedCommands)
    val matcher = regex.pattern.matcher(actualCommands)
    assert(matcher.matches())
  }

  test("all hadoop related environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envWithAllHadoop
    }
    matchActualAndExpected(builder)
  }

  test("only FLINK_HADOOP_CLASSPATH environment variables are configured") {
    val builder = new FlinkProcessBuilder("vinoyang", conf) {
      override def env: Map[String, String] = envDefault +
        (FLINK_HADOOP_CLASSPATH_KEY -> s"${File.separator}hadoop")
    }
    matchActualAndExpected(builder)
  }
}
