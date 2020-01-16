package com.moussatech.etl

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

class ETLManager private constructor() {
    init {}

    companion object: SingletonHolder<ETLManager>(::ETLManager)

    private var spark: SparkSession = SparkSession.builder()
        .appName("Kotlin Spark SQL ETL")
        .config("spark.master", "local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.debug.maxToStringFields", "500")
        .orCreate

    fun readFromDB(databaseConnectionInformation: DatabaseConnectionInformation, tableNamesToRead: List<String>, afterReadClosure: (Dataset<Row>) -> Void) {
        val sqlCon = this.spark.sqlContext()
        for (tableName in tableNamesToRead) {
            val jdbcDataSet = sqlCon.read().jdbc(databaseConnectionInformation.DBURL, tableName, databaseConnectionInformation.DBProperties)
            afterReadClosure(jdbcDataSet)
        }
    }

    fun saveDataSetToSpark(df: Dataset<Row>, tableName: String) {
        df.createOrReplaceTempView(tableName)
    }

    fun runSQLOnSpark(sql: String, afterSQLRan: (Dataset<Row>) -> Void) {
        val result = this.spark.sql(sql)
        afterSQLRan(result)
    }

    fun saveDataToDB(df: Dataset<Row>, databaseConnectionInformation: DatabaseConnectionInformation, tableName: String) {
        df.write().mode(SaveMode.Append).jdbc(databaseConnectionInformation.DBURL, tableName, databaseConnectionInformation.DBProperties)
    }
}