package ru.headhunter;
import java.sql.*;
import java.util.List;

public class BatchInserter {
    private static final int BATCH_SIZE = 1000; // Размер пакета для вставки
    private static final Config config = new Config("application.properties");
    private static final String URL = config.getProperty("database.url");
    private static final String USERNAME = config.getProperty("database.username");
    private static final String PASSWORD = config.getProperty("database.password");

    public static void batchInsertEmployees(List<Employee> employees) throws SQLException {
        DatabaseManager dbManager = new DatabaseManager(URL, USERNAME, PASSWORD);
        Connection conn = null;
        PreparedStatement stmt = null;

        long startTime = System.currentTimeMillis();
        try {
            conn = dbManager.getConnection();
            conn.setAutoCommit(false); // Отключаем автокоммит

            String query = "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)";
            stmt = conn.prepareStatement(query);

            int count = 0;
            for (Employee employee : employees) {
                stmt.setString(1, employee.getFullName());
                stmt.setDate(2, java.sql.Date.valueOf(employee.getBirthDate()));
                stmt.setString(3, employee.getGender());
                stmt.addBatch();

                if (++count % BATCH_SIZE == 0) {
                    System.out.println("Executing batch at count " + count);
                    stmt.executeBatch();
                }
            }
            // Выполнение последнего пакета
            if (count % BATCH_SIZE != 0) {
                System.out.println("Executing final batch at count " + count);
                stmt.executeBatch();
            }
            conn.commit(); // Фиксируем изменения
        } catch (SQLException e) {
            e.printStackTrace();
            if (conn != null && !conn.getAutoCommit()) {
                try {
                    conn.rollback(); // Откатываем изменения в случае ошибки
                } catch (SQLException rollbackEx) {
                    rollbackEx.printStackTrace();
                }
            }
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) {
                    if (!conn.getAutoCommit()) {
                        conn.setAutoCommit(true); // Включаем автокоммит обратно
                    }
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for batch insertion: " + (endTime - startTime) + " ms");
    }
}
