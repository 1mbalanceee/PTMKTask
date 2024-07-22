package ru.headhunter;

import java.time.LocalDate;
import java.util.Random;

public class EmployeeFactory {
    private static final String[] FIRST_NAMES = {"John", "Jane", "Alex", "Emily", "Michael", "Sarah"};
    private static final String[] LAST_NAMES = {"Smith", "Johnson", "Brown", "Williams", "Jones", "Garcia"};
    private static final String[] GENDERS = {"Male", "Female"};
    private static final Random RANDOM = new Random();

    public static Employee createRandomEmployee() {
        String fullName = LAST_NAMES[RANDOM.nextInt(LAST_NAMES.length)] + " " +
                FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)] + " " +
                FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)];
        LocalDate birthDate = LocalDate.of(RANDOM.nextInt(50) + 1970, RANDOM.nextInt(12) + 1, RANDOM.nextInt(28) + 1);
        String gender = GENDERS[RANDOM.nextInt(GENDERS.length)];
        return new Employee(fullName, birthDate, gender);
    }

    public static Employee createMaleEmployeeWithLastNameStartingWithF() {
        String fullName = "F" + LAST_NAMES[RANDOM.nextInt(LAST_NAMES.length - 1) + 1].substring(1) + " " +
                FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)] + " " +
                FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)];
        LocalDate birthDate = LocalDate.of(RANDOM.nextInt(50) + 1970, RANDOM.nextInt(12) + 1, RANDOM.nextInt(28) + 1);
        return new Employee(fullName, birthDate, "Male");
    }
}

