package ru.headhunter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.sql.Connection;

public class Main {
    private static final int THREAD_COUNT = 10;
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Конфигурация для подключения к базе данных
        Config config = new Config("application.properties");
        String url = config.getProperty("database.url");
        String username = config.getProperty("database.username");
        String password = config.getProperty("database.password");

        // Создание экземпляра DatabaseManager
        DatabaseManager dbManager = new DatabaseManager(url, username, password);

        try {
            dbManager.connect(); // Подключение к базе данных
            System.out.println("Connected to database successfully.");

            while (true) {
                // Вывод меню
                System.out.println("Choose an option:");
                System.out.println("1 - Create table");
                System.out.println("2 - Add employee");
                System.out.println("3 - List employees");
                System.out.println("4 - Fill table");
                System.out.println("5 - Select employees");
                System.out.println("6 - Optimize database");
                System.out.println("7 - Exit");

                // Чтение выбора пользователя
                int choice = scanner.nextInt();
                scanner.nextLine(); // Очистка буфера после nextInt()

                switch (choice) {
                    case 1:
                        createTable(dbManager);
                        break;
                    case 2:
                        enterEmployeeIntoDatabase(scanner, dbManager);
                        break;
                    case 3:
                        listEmployees(dbManager);
                        break;
                    case 4:
                        fillTable(dbManager);
                        break;
                    case 5:
                        selectEmployees(dbManager);
                        break;
                    case 6:
                        optimizeDatabase(dbManager);
                        break;
                    case 7:
                        System.out.println("Exiting...");
                        return; // Завершение работы программы
                    default:
                        System.out.println("Invalid choice, please select again.");
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                dbManager.disconnect(); // Отключение от базы данных
                System.out.println("Disconnected from database.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createTable(DatabaseManager dbManager) {
        String query = "CREATE TABLE IF NOT EXISTS employees (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "full_name VARCHAR(255) NOT NULL," +
                "birth_date DATE NOT NULL," +
                "gender VARCHAR(10) NOT NULL)";
        dbManager.executeUpdate(query);
        System.out.println("Table created successfully.");
    }

    private static void enterEmployeeIntoDatabase(Scanner scanner, DatabaseManager dbManager) {
        System.out.print("Enter full name: ");
        String fullName = scanner.nextLine();
        System.out.print("Enter birth date (YYYY-MM-DD): ");
        LocalDate birthDate = LocalDate.parse(scanner.nextLine());
        System.out.print("Enter gender: ");
        String gender = scanner.nextLine();

        // Создание объекта Employee
        Employee employee = new Employee(fullName, birthDate, gender);

        // Сохранение объекта в базе данных
        employee.saveToDatabase(dbManager);

        System.out.println("Employee added successfully: " + employee);
    }

    private static void fillTable(DatabaseManager dbManager) {
        System.out.println("Starting to fill the table with employees.");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<Void>> futures = new ArrayList<>();

        int totalEmployees = 100000; //TODO 1000000
        int batchSize = totalEmployees / THREAD_COUNT;
        int startIndex = 0;

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int start = startIndex;
            final int end = start + batchSize;
            int finalI = i;
            Future<Void> future = executor.submit(() -> {
                System.out.println("Thread " + Thread.currentThread().getId() + " starting to generate employees from index " + start);
                List<Employee> employees = new ArrayList<>();
                for (int j = start; j < end; j++) {
                    employees.add(EmployeeFactory.createRandomEmployee());
                }
                // Заполняем 100 специальных записей в последнем потоке
                if (finalI == THREAD_COUNT - 1) {
                    System.out.println("Thread " + Thread.currentThread().getId() + " adding special employees.");
                    for (int k = 0; k < 100; k++) {
                        employees.add(EmployeeFactory.createMaleEmployeeWithLastNameStartingWithF());
                    }
                }
                System.out.println("Thread " + Thread.currentThread().getId() + " about to batch insert employees.");
                batchInsertEmployees(dbManager, employees);
                System.out.println("Thread " + Thread.currentThread().getId() + " finished inserting employees.");
                return null;
            });
            futures.add(future);
            startIndex = end; // Обновляем начальный индекс для следующего потока
        }

        // Ожидаем завершения всех потоков
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        System.out.println("Table filled with employees successfully.");
    }

    private static void batchInsertEmployees(DatabaseManager dbManager, List<Employee> employees) {
        String query = "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)";

        // Используем try-with-resources для автоматического закрытия ресурсов
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            conn.setAutoCommit(false); // Отключаем автокоммит

            int count = 0;

            for (Employee employee : employees) {
                stmt.setString(1, employee.getFullName());
                stmt.setDate(2, java.sql.Date.valueOf(employee.getBirthDate()));
                stmt.setString(3, employee.getGender());
                stmt.addBatch();

                if (++count % BATCH_SIZE == 0) {
                    System.out.println("Executing batch at count " + count);
                    stmt.executeBatch(); // Выполняем пакет
                }
            }

            // Выполнение последнего пакета, если он есть
            if (count % BATCH_SIZE != 0) {
                System.out.println("Executing final batch at count " + count);
                stmt.executeBatch();
            }

            conn.commit(); // Фиксируем изменения

        } catch (SQLException e) {
            System.err.println("SQLException occurred: " + e.getMessage());
            // Если соединение открыто, откатываем изменения
            try {
                if (!dbManager.getConnection().isClosed()) {
                    dbManager.getConnection().rollback(); // Откатываем изменения
                }
            } catch (SQLException rollbackEx) {
                rollbackEx.printStackTrace();
            }
        }
    }




    private static void listEmployees(DatabaseManager dbManager) {
        System.out.println("Listing employees...");
        String query = "SELECT full_name, birth_date, gender FROM employees ORDER BY full_name";
        try (ResultSet rs = dbManager.executeQuery(query)) {
            while (rs.next()) {
                String fullName = rs.getString("full_name");
                LocalDate birthDate = LocalDate.parse(rs.getString("birth_date"));
                String gender = rs.getString("gender");
                Employee employee = new Employee(fullName, birthDate, gender);
                System.out.println(employee + ", Age: " + employee.calculateAge());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void selectEmployees(DatabaseManager dbManager) {
        System.out.println("Selecting employees with gender Male and last name starting with 'F'...");
        String query = "SELECT full_name, birth_date, gender FROM employees WHERE gender = 'Male' AND full_name LIKE 'F%'";

        long startTime = System.currentTimeMillis();

        try (ResultSet rs = dbManager.executeQuery(query)) {
            // Заголовок для вывода
            System.out.printf("%-30s %-15s %-10s%n", "Full Name", "Birth Date", "Gender");
            System.out.println("---------------------------------------------------------------");

            while (rs.next()) {
                String fullName = rs.getString("full_name");
                LocalDate birthDate = rs.getDate("birth_date").toLocalDate(); // Преобразуем java.sql.Date в LocalDate
                String gender = rs.getString("gender");

                // Создаем объект Employee (если нужно использовать его для других целей)
                Employee employee = new Employee(fullName, birthDate, gender);

                // Вывод данных на экран
                System.out.printf("%-30s %-15s %-10s%n", fullName, birthDate, gender);
                System.out.println(employee + ", Age: " + employee.calculateAge());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Query execution time: " + (endTime - startTime) + " ms");
    }

    private static void optimizeDatabase(DatabaseManager dbManager) {
        // Оптимизация базы данных
        // Пример: выполнение VACUUM для PostgreSQL
        String query = "VACUUM";
        dbManager.executeUpdate(query);
        System.out.println("Database optimized successfully.");
    }
}
