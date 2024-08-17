package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.*;

public class LambdaRdsHandler implements RequestHandler<Object, String> {

    // RDS接続情報
    private static final String RDS_ENDPOINT = "exampledb.xxxx.us-east-1.rds.amazonaws.com";
    private static final String DATABASE_NAME = "application";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin123";

    // JDBC URLを組み立てる
    private static final String JDBC_URL = "jdbc:mysql://" + RDS_ENDPOINT + ":3306/" + DATABASE_NAME;

    @Override
    public String handleRequest(Object input, Context context) {
        Connection connection = null;
        Statement statement = null;
        String result = "";

        try {
            // RDSデータベースへの接続を確立
            connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);

            // メタデータを取得してテーブルが存在するかを確認
            DatabaseMetaData dbMetaData = connection.getMetaData();
            ResultSet tables = dbMetaData.getTables(null, null, "sample_table", null);
            if (!tables.next()) {
                // テーブルが存在しない場合、作成する
                statement = connection.createStatement();
                String createTableSQL = "CREATE TABLE sample_table (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(255) NOT NULL, " +
                        "age INT NOT NULL)";
                statement.executeUpdate(createTableSQL);
                context.getLogger().log("Table 'sample_table' created successfully.");
            } else {
                context.getLogger().log("Table 'sample_table' already exists.");
            }
            // データを追加するSQLクエリ
            String insertSQL = "INSERT INTO sample_table (name, age) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
            preparedStatement.setString(1, "John Doe");
            preparedStatement.setInt(2, 30);
            int rowsInserted = preparedStatement.executeUpdate();
            // データを取得するSQLクエリを実行
            statement = connection.createStatement();
            String query = "SELECT * FROM sample_table";
            ResultSet resultSet = statement.executeQuery(query);

            // 結果を処理
            while (resultSet.next()) {
                result += "ID: " + resultSet.getInt("id") + ", ";
                result += "Name: " + resultSet.getString("name") + ", ";
                result += "Age: " + resultSet.getInt("age") + "\n";
            }

        } catch (SQLException e) {
            context.getLogger().log("Error: " + e.getMessage());
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                context.getLogger().log("Error closing resources: " + e.getMessage());
            }
        }

        return result;
    }
}
