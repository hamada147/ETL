package com.moussatech.etl

import java.util.Properties

data class DatabaseConnectionInformation(val host: String, val port: String?, val username: String, val password: String, val jdbcType: String) {
    val DBURL: String
        get() {
            var url = "jdbc:${this.jdbcType}://${this.host}"
            if (!this.port.isNullOrEmpty()) {
                url += ":${this.port}"
            }
            return url
        }

    val DBProperties: Properties
        get() {
            val connectionProperties = Properties()
            connectionProperties["user"] = this.username
            connectionProperties["password"] =this.password
            return connectionProperties
        }
}