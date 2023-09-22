package com.alibaba.graphar.example

import org.apache.spark.sql.SparkSession
import com.alibaba.graphar.VertexInfo
import com.alibaba.graphar.reader.VertexReader
import com.alibaba.graphar.AdjListType
import com.alibaba.graphar.reader.EdgeReader
import com.alibaba.graphar.EdgeInfo
import org.apache.spark.sql.functions.{col, _}

object GraphArPushDown {

  def main(args: Array[String]): Unit = {
    val enablePushDown = false
    val spark = SparkSession
      .builder()
      .appName("read GraphAr with(out) filter pushdown")
      .config("spark.sql.parquet.filterPushdown", enablePushDown)
      .config("spark.sql.optimizer.nestedSchemaPruning.enabled", enablePushDown)
      .master("local[*]")
      .getOrCreate()

    val start = System.nanoTime()
    // construct the vertex information
    // val prefix = args(1)
    val prefix = "/home/simple/ldbc-sf100/"
    val vertex_yaml = prefix + "person.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)
    // construct the vertex reader
    val vertex_reader = new VertexReader(prefix, vertex_info, spark)

    // test reading the number of vertices
    assert(vertex_reader.readVerticesNumber() == 448626)
    val vertex_pg = vertex_info.getPropertyGroup("id")
    var newColumns = (0 to 1).map(i => {
      val columnName = s"gender$i"
      val newColumn = concat(
        col("firstName"),
        lit("-"),
        col("lastName"),
        lit("-"),
        col("gender")
      ).as(columnName)
      newColumn
    })
    newColumns +:= col("firstName")
    newColumns +:= col("lastName")
    newColumns +:= col("gender")

    var vertex_df = vertex_reader
      .readVertexPropertyGroup(vertex_pg, addIndex = false)
      .select(newColumns: _*)
      .filter(col("gender") === "male")
    vertex_df.explain()
    assert(vertex_df.count() == 223843)

    // construct the edge information
    val edge_yaml = prefix + "person_knows_person.yml"
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml, spark)
    // construct the edge reader
    var adj_list_type = AdjListType.ordered_by_source
    var edge_reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)

    assert(edge_reader.readEdgesNumber() == 19941198)
    var edge_df = edge_reader.readEdges(false)
    assert(edge_df.count() == 19941198)

    adj_list_type = AdjListType.ordered_by_dest
    edge_reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)

    assert(edge_reader.readEdgesNumber() == 19941198)
    edge_df = edge_reader.readEdges(false)
    assert(edge_df.count() == 19941198)

    println(s"execution time: ${(System.nanoTime() - start) / 1e9} s")
  }
}
