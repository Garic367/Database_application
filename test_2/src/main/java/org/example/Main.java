package org.example;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Main {
    private static final int count = 500;
    private static final int size = 1000;
    private static final int numProcessors = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {
        long startTime;
        System.out.println(numProcessors);
        // Sequential write
        startTime = System.currentTimeMillis();
        insertDataSequential(count);
        long sequentialWriteTime = System.currentTimeMillis() - startTime;
        System.out.println("Последовательная запись: " + sequentialWriteTime + "мс");

        // Concurrent write
        startTime = System.currentTimeMillis();
        insertDataConcurrent(count);
        long concurrentWriteTime = System.currentTimeMillis() - startTime;
        System.out.println("Параллельная запись: " + concurrentWriteTime + "мс");

        // Read data and calculate average salary using Stream API
        calculateAverageSalaryByDepartment();
    }

    private static void insertDataSequential(int count) {
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/reglog", "root", "root")) {
            String insertQuery = "INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                for (int i = 0; i < count; i++) {
                    String name = "Employee " + i;
                    String department = "Department " + (i % 3);
                    double salary = (int) (Math.random() * (1000 - 100 + 1) + 100);;

                    statement.setString(1, name);
                    statement.setString(2, department);
                    statement.setDouble(3, salary);
                    statement.executeUpdate();
                }
                statement.executeBatch();
                statement.addBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertDataConcurrent(int numRecords) {
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/reglog", "root", "root")) {
            connection.setAutoCommit(false);

            String insertQuery = "INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);

                for (int i = 0; i < numRecords; i++) {
                    String name = "Name " + i;
                    String department = "Department " + (i % 3);
                    double salary = (int) (Math.random() * (1000 - 100 + 1) + 100);;

                    statement.setString(1, name);
                    statement.setString(2, department);
                    statement.setDouble(3, salary);
                    statement.addBatch();

                    if ((i + 1) % size == 0) {
                        executorService.execute(() -> {
                            try {
                                statement.executeBatch();
                                connection.commit();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        });
                    }
                }
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                connection.commit();
            }
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void calculateAverageSalaryByDepartment() {
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/reglog", "root", "root");
             Statement statement = connection.createStatement()) {
            String selectQuery = "SELECT department, salary FROM employees";
            ResultSet resultSet = statement.executeQuery(selectQuery);

            Stream<ResultSet> resultSetStream = StreamSupport.stream(
                    new ResultSetSpliterator(resultSet), false);

            resultSetStream
                    .collect(Collectors.groupingBy(
                            rs -> {
                                try {
                                    return rs.getString("department");
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            Collectors.averagingDouble(rs -> {
                                try {
                                    return rs.getDouble("salary");
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    ))
                    .forEach((department, averageSalary) ->
                            System.out.println(department + ": Средняя зарплата по департаменту: "+String.format("%.2f",averageSalary) ));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
