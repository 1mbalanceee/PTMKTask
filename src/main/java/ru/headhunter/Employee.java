package ru.headhunter;
import java.time.LocalDate;
import java.time.Period;

public class Employee {
    private String fullName;
    private LocalDate birthDate;
    private String gender;

    public Employee(String fullName, LocalDate birthDate, String gender) {
        this.fullName = fullName;
        this.birthDate = birthDate;
        this.gender = gender;
    }

    public String getFullName() {
        return fullName;
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public String getGender() {
        return gender;
    }

    public int calculateAge() {
        return Period.between(birthDate, LocalDate.now()).getYears();
    }

    public void saveToDatabase(DatabaseManager dbManager) {
        String query = "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)";
        dbManager.executeUpdate(query, fullName, birthDate.toString(), gender);
    }

    @Override
    public String toString() {
        return "Employee{" +
                "fullName='" + fullName + '\'' +
                ", birthDate=" + birthDate +
                ", gender='" + gender + '\'' +
                '}';
    }

}

