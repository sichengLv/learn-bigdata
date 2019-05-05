package com.lv.bigdata.action.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.*;

/**
 * Jdbc输出流
 *
 * @author lvsicheng
 * @date 2019-05-05 17:09
 */
public class JdbcWriter extends ForeachWriter<Row> {

    private static final Logger LOGGER = Logger.getLogger(JdbcWriter.class);

    private static Statement statement;

    private static Connection connection;

    private String jdbcUrl;

    private String userName;

    private String password;

    public JdbcWriter(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public boolean open(long partitionId, long version) {
        try {
            connection = DriverManager.getConnection(this.jdbcUrl, this.userName, this.password);
            statement = connection.createStatement();

            return true;
        } catch (SQLException e) {
            LOGGER.error("Get connection exception", e);
        }
        return false;
    }

    @Override
    public void process(Row value) {
        String topicName = String.valueOf(value.getAs("topicName")).replaceAll("[\\[\\]]", "");
        Long topicCount = Long.valueOf(value.getAs("topicCount"));

        String querySql = "select * from click_event where topic = '" + topicName + "'";
        String updateSql = "update click_event set count = " + topicCount + " where topic = '" + topicName + "'";
        String insertSql = "insert into click_event(topic, count)" + " values('" + topicName + "', " + topicCount + ")";

        ResultSet resultSet = null;
        try {
            LOGGER.info("Exec query: " + querySql);
            resultSet = statement.executeQuery(querySql);

            if (resultSet.next()) {
                LOGGER.info("Exec update: " + querySql);
                statement.executeUpdate(updateSql);
            } else {
                LOGGER.info("Exec insert: " + querySql);
                statement.execute(insertSql);
            }
        } catch (SQLException e) {
            LOGGER.error("Exec sql exception", e);
        }

    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            if (null != statement) statement.close();
            if (null != connection) connection.close();
        } catch (SQLException e) {
            LOGGER.error("Close exception", e);
        }
    }

}
