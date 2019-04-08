package com.tomekl007

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD


case class Record(amount: Double, desc: String)


class MultipliedRDD(prev:RDD[Record], multiplier: Double) extends RDD[Record](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[Record] = {
    firstParent[Record].iterator(split, context).map(salesRecord => {
      val mult = salesRecord.amount * multiplier
      new Record(mult, salesRecord.desc)
    })

  }

  override  protected def getPartitions: Array[Partition] = firstParent[Record].partitions

}
