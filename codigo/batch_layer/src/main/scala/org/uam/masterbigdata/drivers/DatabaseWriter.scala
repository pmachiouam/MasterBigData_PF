package org.uam.masterbigdata.drivers

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

trait DatabaseWriter {
  def saveDataFrameInPostgresSQL(df: DataFrame, tableName: String): Unit = {
    val props = Map(
      JDBCOptions.JDBC_DRIVER_CLASS -> "org.postgresql.Driver"
      , JDBCOptions.JDBC_URL-> "jdbc:postgresql://postgres:5432/tracking"
      , "user" -> "docker"
      , "password" -> "docker"
      , "stringtype" -> "unspecified" //Usar string para tipo uuid de postgresql
    )

    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .options(props)
      .option("dbtable", s"public.$tableName").save()
  }
}
