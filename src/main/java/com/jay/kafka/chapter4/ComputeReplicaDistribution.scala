package com.jay.kafka.chapter4

import kafka.admin.{AdminUtils, BrokerMetadata}

/**
  * 计算分区副本分配
  *
  * @author xuweijie
  */
object ComputeReplicaDistribution {
  val partitions = 3
  var replicaFactor = 2

  def main(args: Array[String]): Unit = {
    val brokerMetadatas = List(new BrokerMetadata(0, Option("rack1")), new BrokerMetadata(1, Option("rack1")),
      new BrokerMetadata(2, Option("rack1")))
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicaFactor)
    print(replicaAssignment)
  }

}
