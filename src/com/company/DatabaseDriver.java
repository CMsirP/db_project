package com.company;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Scanner;

public class DatabaseDriver {
    // CLI will be implemented in final version, for now, user will hardcode in their database
    // url, pw, username, as well as their File path strings
    private Connection connection = null;
    private static Connection connection2 = null;
    private static DatabaseDriver instance = null;
    private static String url = "jdbc:mysql://localhost:3306/hospital_system"; // database string url
    private static String username = "root"; // db username
    private static String password = "M@gn3t1cGammaRay"; // db password

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
	// connect to database
        DatabaseDriver database = DatabaseDriver.getInstance();

        // query list
        List<String> queries = new ArrayList<>();
        // Room Utilization (1.1 - 1.3)
        queries.add("SELECT room_number, lastname, arrival_date FROM patients \n" +
                "JOIN admitted_patients ON patients.patient_id=admitted_patients.patient_id \n" +
                "WHERE discharge_date is null;");
        queries.add("select * from (VALUES ROW (1) , ROW (2) , ROW (3), ROW (4), ROW (5), \n" +
                "ROW (6), ROW (7), ROW (8), ROW (9), ROW (10), ROW (11), ROW (12), ROW (13), ROW (14), ROW (15),  ROW (16),  ROW (17),  ROW (18),  ROW (19),  ROW (20)) L(room_number) \n" +
                "\twhere not exists (select room_number from patients V where V.room_number = L.room_number);");
        queries.add("SELECT patients.firstname, patients.lastname, patients.room_number, \n" +
                "admitted_patients.arrival_date\n" +
                "FROM admitted_patients, patients\n" +
                "WHERE patients.patient_id = admitted_patients.patient_id\n" +
                "AND discharge_date IS NULL\n" +
                "UNION\n" +
                "SELECT null, null, room_number, null\n" +
                "FROM (VALUES ROW (1) , ROW (2) , ROW (3), ROW (4), ROW (5), ROW (6), ROW \n" +
                "(7), ROW (8),\n" +
                "ROW (9), ROW (10), ROW (11), ROW (12), ROW (13), ROW (14), ROW (15), ROW \n" +
                "(16), ROW (17),\n" +
                "ROW (18), ROW(19), ROW (20)) R(room_number)\n" +
                "WHERE NOT EXISTS (\n" +
                "SELECT room_number\n" +
                "FROM patients, admitted_patients\n" +
                "WHERE patients.room_number = R.room_number\n" +
                "AND patients.patient_id = admitted_patients.patient_id\n" +
                "AND discharge_date IS NULL);");
        // Patient Information (2.1 - 2.8)
        queries.add("SELECT * \n" +
                "FROM patients;");
        queries.add("SELECT patient_id, lastname \n" +
                "FROM patients JOIN admitted_patients USING(patient_id)\n" +
                "WHERE admitted_patients.discharge_date is null;");
        queries.add("SELECT patients.patient_id, lastname\n" +
                "FROM patients JOIN admitted_patients ON patients.patient_id=admitted_patients.patient_id\n" +
                "WHERE (discharge_date BETWEEN '2019-02-01 05:00:00' and '2021-03-01 05:00:00');");
        queries.add("SELECT patients.patient_id, lastname\n" +
                "FROM patients JOIN admitted_patients ON patients.patient_id=admitted_patients.patient_id\n" +
                "WHERE (arrival_date BETWEEN '2019-02-01 05:00:00' and '2021-03-01 05:00:00');");
        queries.add("SELECT initial_diagnosis, P.firstname, P.lastname\n" +
                "\tFROM admitted_patients A JOIN patients P USING(patient_id)\n" +
                "\tWHERE P.lastname = \"Henry\" OR P.patient_id = 5;");
        queries.add("SELECT DISTINCT patients.firstname, patients.lastname, \n" +
                "treatments.treatment_name, admitted_patients.arrival_date\n" +
                "FROM treatments LEFT JOIN admitted_patients ON \n" +
                "admitted_patients.patient_id = treatments.patient_id\n" +
                "LEFT JOIN patients on patients.patient_id = admitted_patients.patient_id\n" +
                "WHERE patients.lastname = 'Hunt'\n" +
                "ORDER BY admitted_patients.arrival_date DESC;");
        queries.add("SELECT DISTINCT p.patient_id, p.firstname, p.lastname, \n" +
                "ap.initial_diagnosis,\n" +
                "e.lastname AS doctor_lastname\n" +
                "FROM admitted_patients ap\n" +
                "INNER JOIN admitted_patients ap1 on ap1.patient_id = ap.patient_id\n" +
                "LEFT JOIN patients p on p.patient_id = ap.patient_id\n" +
                "LEFT JOIN employees e on e.employee_id = p.employee_id\n" +
                "WHERE ap.arrival_date BETWEEN ap1.discharge_date AND ap1.discharge_date + \n" +
                "INTERVAL 30 DAY");
        queries.add("WITH date_calcs AS (\n" +
                "\tSELECT patient_id, datediff(LEAD(arrival_date) OVER (PARTITION BY patient_id ORDER BY arrival_date), discharge_date) AS time_span\n" +
                "    FROM admitted_patients\n" +
                ")\n" +
                "SELECT a.patient_id, COUNT(a.admit_id)\n" +
                "OVER (PARTITION by a.patient_id) AS 'Total Admissions', MIN(dc.time_span), MAX(dc.time_span), AVG(dc.time_span)\n" +
                "FROM admitted_patients a\n" +
                "INNER JOIN date_calcs dc ON dc.patient_id= a.patient_id GROUP BY a.patient_id, a.admit_id;");
        // Diagnosis and Treatment Information (3.1 - 3.5)
        queries.add("SELECT\n" +
                "\tinitial_diagnosis, COUNT(*) AS 'Total # of Occurrences'\n" +
                "FROM \n" +
                "\tadmitted_patients JOIN patients USING(patient_id)\n" +
                "GROUP BY initial_diagnosis;");
        queries.add("SELECT\n" +
                "\tinitial_diagnosis, COUNT(*) AS 'Total # of Occurrences'\n" +
                "FROM \n" +
                "\tadmitted_patients JOIN patients USING(patient_id)\n" +
                "GROUP BY initial_diagnosis;");
        queries.add("SELECT treatments.treatment_name, COUNT(*)\n" +
                "FROM treatments, admitted_patients\n" +
                "WHERE treatments.patient_id = admitted_patients.patient_id\n" +
                "AND discharge_date IS NULL\n" +
                "GROUP BY treatment_name\n" +
                "ORDER BY treatments.timestamp DESC;");
        queries.add("SELECT admitted_patients.initial_diagnosis\n" +
                "FROM admitted_patients, patients\n" +
                "WHERE patients.patient_id = admitted_patients.patient_id\n" +
                "HAVING MAX(admit_id);");
        queries.add("SELECT treatment_id, lastname, patients.employee_id\n" +
                "FROM treatments\tJOIN patients USING(patient_id)\n" +
                "WHERE treatment_id = 14;");
        // Employee Information (4.1 - 4.5)
        queries.add("SELECT lastname, firstname, category\n" +
                "FROM employees\n" +
                "ORDER BY lastname, firstname ASC;");
        queries.add("SELECT e.firstname, e.lastname\n" +
                "FROM admitted_patients ap\n" +
                "LEFT JOIN patients p on p.patient_id = ap.patient_id\n" +
                "LEFT JOIN employees e on e.employee_id = p.employee_id\n" +
                "LEFT JOIN admitted_patients ap1 on ap1.patient_id = ap.patient_id\n" +
                "WHERE ap.arrival_date BETWEEN ap1.discharge_date AND ap1.discharge_date + \n" +
                "INTERVAL 365 DAY\n" +
                "GROUP BY e.employee_id;");
        queries.add("select initial_diagnosis, count(initial_diagnosis) \n" +
                "from admitted_patients join patients using(patient_id)\n" +
                "where employee_id = 18\n" +
                "group by initial_diagnosis\n" +
                "order by count(initial_diagnosis) desc;");
        queries.add("select treatment_name, count(treatment_name) as 'total'\n" +
                "from employees join treatments using(employee_id)\n" +
                "where employee_id = 18\n" +
                "group by treatment_name\n" +
                "order by count(treatment_name) desc;");
        queries.add("select patient_id, employee_id\n" +
                "from treatments;");

        // CLI Query Loop
        Scanner sc = new Scanner(System.in);
        String selection = "";
        // Prompt user to load files or skip
        System.out.println("Welcome. Enter y to load files or any other character to skip to queries: ");
        if ((selection = sc.next()).equalsIgnoreCase("y")) {
            // read content from file
            database.readPersonFile("C:\\Users\\cmp0106\\IdeaProjects\\db_project\\src\\com\\company" +
                    "\\person_file.txt"); // read in db person file

            database.readDoctorFile("C:\\Users\\cmp0106\\IdeaProjects\\db_project\\src\\com\\company" +
                    "\\doctor_file.txt"); // read in db assigned doctors file

            database.readTreatmentFile("C:\\Users\\cmp0106\\IdeaProjects\\db_project\\src\\com\\company" +
                    "\\treatment_file.txt"); // read in db treatment file
        }
        printMenu();
        while (selection != "-1") {
            selection = sc.next();
            int index = Integer.parseInt(selection.trim());
            if (index >= 0 && index < queries.size()) {
                runQuery(queries, index);
            }
            System.out.println("Would you like to make another query? Enter y to continue or any other " +
                    "character to quit.");
            selection = sc.next();
            if (selection.equalsIgnoreCase("y")) {
                printMenu();
            }
            else {
                selection = "-1";
            }
        }
        System.out.println("Exiting program. Goodbye.");
    }

    private static void runQuery(List<String> queries, int index) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection2 = DriverManager.getConnection(url, username, password);
            Statement statement = connection2.createStatement();
            ResultSet resultSet = statement.executeQuery(queries.get(index));
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            StringBuilder sb = new StringBuilder();

            // first loop to get all tuples
            while(resultSet.next()) {
                // second loop to get all columns in each tuple
                for(int i=1; i<=columnCount; i++) {
                    String column_title = resultSetMetaData.getColumnName(i);
                    // Data type object since the columns can be int, string, or datetime
                    Object column_value = resultSet.getObject(column_title);
                    System.out.println(column_value + " ");
                }
                System.out.println("");
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void printMenu() {
        System.out.println("\nSelect the query you would like to run: ");
        System.out.println("| Room Utilization |" +
                "\n\t[0] List the rooms that are occupied, " +
                "along with the associated patient names and the date the patient was admitted." +
                "\n\t[1] List the rooms that are currently unoccupied." +
                "\n\t[2] List all rooms in the hospital along with patient names and admission dates " +
                "for those that are occupied.");
        System.out.println("\n| Patient Information |" +
                "\n\t[3] List all patients in the database, with full personal information." +
                "\n\t[4] List all patients currently admitted to the hospital. List only " +
                "patient identification number and name." +
                "\n\t[5] List all patients who were discharged in a given date range. " +
                "List only patient identification number and name." +
                "\n\t[6] List all patients who were admitted within a given date range. " +
                "List only patient identification number and name." +
                "\n\t[7] For a given patient (either patient identification number or name), " +
                "list all admissions to the hospital along with the diagnosis for each admission." +
                "\n\t[8] For a given patient (either patient identification number or name), " +
                "list all treatments that were administered. Group treatments by admissions. " +
                "List admissions in descending chronological order, and list treatments " +
                "in ascending chronological order within each admission." +
                "\n\t[9] List patients who were admitted to the hospital within 30 days of their " +
                "last discharge date. For each patient list their patient identification number, " +
                "name, diagnosis, and admitting doctor." +
                "\n\t[10] For each patient that has ever been admitted to the hospital, " +
                "list their total number of admissions, average duration of each admission, " +
                "longest span between admissions, shortest span between admissions, " +
                "and average span between admissions.");
        System.out.println("\n| Diagnosis and Treatment Information |" +
                "\n\t[11] List the diagnoses given to patients, in descending order of occurrences. " +
                "List diagnosis identification number, name, and total occurrences of each diagnosis." +
                "\n\t[12] List the diagnoses given to hospital patients, in descending order of occurrences. " +
                "List diagnosis identification number, name, and total occurrences of each diagnosis." +
                "\n\t[13] List the treatments performed on admitted patients, in descending order " +
                "of occurrences. List treatment identification number, name, and total number of " +
                "occurrences of each treatment." +
                "\n\t[14] List the diagnoses associated with patients who have the highest occurrences " +
                "of admissions to the hospital, in ascending order or correlation." +
                "\n\t[15] For a given treatment occurrence, list the patient name and the doctor " +
                "who ordered the treatment.");
        System.out.println("\n| Employee Information |" +
                "\n\t[16] List all workers at the hospital, in ascending last name, first name order. " +
                "For each worker, list their, name, and job category." +
                "\n\t[17] List the primary doctors of patients with a high admission rate (at least 4 " +
                "admissions within a one-year time frame)." +
                "\n\t[18] For a given doctor, list all associated diagnoses in descending order of occurrence. " +
                "For each diagnosis, list the total number of occurrences for the given doctor." +
                "\n\t[19] For a given doctor, list all treatments that they ordered in descending order " +
                "of occurrence. For each treatment, list the total number of occurrences for the given doctor." +
                "\n\t[20] List employees who have been involved in the treatment of every admitted patient.");
        System.out.println("\n\n[-1] ---- EXIT ----");
    }

    private void readPersonFile(String fileURL) throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileURL));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            String[] arrayPerson = line.split(",");
            if(arrayPerson[0].equals("P")) {
                insertPatients(line);
            } else {
                insertEmployees(line);
            }
        }
    }
    private void readTreatmentFile(String fileURL) throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileURL));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            insertTreatments(line);
        }
    }
    private void readDoctorFile(String fileURL) throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileURL));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            insertAdditionalDoctors(line);
        }
    }

    private void insertAdditionalDoctors(String line) throws SQLException {
        String[] arrayDoctor = line.split(",");
        int pat_id = checkIfPatientExists(arrayDoctor[0].trim());
        int doc_id = getPrimaryDoctorIdFromLastname(arrayDoctor[1].trim());

        String assignedDoctorInsertQuery = "INSERT INTO assigned_doctors " +
                "VALUES(?, ?)";
        PreparedStatement preparedStatement =
                connection.prepareStatement(assignedDoctorInsertQuery);
        preparedStatement.setInt(1, pat_id);
        preparedStatement.setInt(2, doc_id);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private void insertTreatments(String line) throws SQLException {
        String[] arrayTreatment = line.split(",");
        int pat_id = checkIfPatientExists(arrayTreatment[0].trim());
        int doc_id = getPrimaryDoctorIdFromLastname(arrayTreatment[1].trim());
        String treat_type = arrayTreatment[2].trim();
        String treat_name = arrayTreatment[3].trim();
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-M-d HH:mm",
                Locale.ENGLISH);
        String raw_time = arrayTreatment[4].trim();
        LocalDateTime timestamp;
        try {
            timestamp = LocalDateTime.parse(raw_time, f);
        }
        catch(DateTimeParseException w) {
            timestamp = null;
        }

        String treatmentInsertQuery = "INSERT INTO treatments VALUES(default, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement =
                connection.prepareStatement(treatmentInsertQuery);
        preparedStatement.setInt(1, pat_id);
        preparedStatement.setInt(2, doc_id);
        preparedStatement.setString(3, treat_name);
        preparedStatement.setString(4, treat_type);
        preparedStatement.setObject(5, timestamp);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }
    private void insertEmployees(String line) throws SQLException {
        String[] arrayEmployee = line.split(",");
        // check if employee already exists in the database
        Boolean flag = checkIfEmployeeExists(arrayEmployee[2]);
        if(flag) {
            System.out.println("Insert error. Employee already exists!");
        } else {
            String category = switch (arrayEmployee[0].trim()) {
                case "D" -> "doctor";
                case "A" -> "administrator";
                case "N" -> "nurse";
                case "T" -> "technician";
                default -> "";
            };
            String firstname = arrayEmployee[1].trim();
            String lastname = arrayEmployee[2].trim();
            String employeeInsertQuery = "INSERT INTO employees " +
                    "VALUES(default, ?, ?, ?)";
                PreparedStatement preparedStatement =
                        connection.prepareStatement(employeeInsertQuery);
                preparedStatement.setString(1, firstname);
                preparedStatement.setString(2, lastname);
                preparedStatement.setString(3, category);
                preparedStatement.executeUpdate();
                preparedStatement.close();
        }
    }
    private Boolean checkIfEmployeeExists(String lastname) throws SQLException {
        boolean flag = false;
        String checkEmployee = "SELECT * FROM employees WHERE lastname = ?";
        PreparedStatement preparedStatement =
                connection.prepareStatement(checkEmployee);
        preparedStatement.setString(1, lastname);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.next()) {
            flag = true;
        }
        return flag;
    }
    private void insertPatients(String line) throws SQLException {
        // if the person is patient
        // check if patient already exists in the table
        // if yes, add only in treatment table
        String[] arrayPerson = line.split(",");
        int patient_id = -1;
        String firstname = arrayPerson[1].trim();
        String lastname = arrayPerson[2].trim();
        patient_id = checkIfPatientExists(lastname);
        // initial diagnosis
        String initial_diagnosis = arrayPerson[9].trim();
        // convert string to localdate format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-M-d HH:mm",
                Locale.ENGLISH);
        LocalDateTime arrival_date = LocalDateTime.parse(arrayPerson[10].trim(),
                formatter);
        LocalDateTime discharge_date;
        try {
            discharge_date = LocalDateTime.parse(arrayPerson[11].trim(),
                    formatter);
        }
        catch(DateTimeParseException w) {
            discharge_date = null;
        }
        if(patient_id != -1) {
            System.out.println("Patient already exists! Adding admitted patient " +
                    "information.");
        } else {
            // room number
            int room_number = Integer.parseInt(arrayPerson[3].trim());
            // emergency contact
            int emergency_contact_id = insertEmergencyContact(arrayPerson[4].trim(),
                    arrayPerson[5].trim());
            // insurance
            int insurance_id = insertInsurance(arrayPerson[6].trim(), arrayPerson[7].trim());
            // get employee id from lastname
            int employee_id = getPrimaryDoctorIdFromLastname(arrayPerson[8].trim());
            // insert into patients table
            String insertPatientsQuery = "INSERT INTO patients VALUES(default, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStatement =
                        connection.prepareStatement(insertPatientsQuery,
                                Statement.RETURN_GENERATED_KEYS);
                preparedStatement.setString(1, firstname);
                preparedStatement.setString(2, lastname);
                preparedStatement.setInt(3, room_number);
                preparedStatement.setInt(4, insurance_id);
                preparedStatement.setInt(5, emergency_contact_id);
                preparedStatement.setInt(6, employee_id);
                preparedStatement.executeUpdate();
                ResultSet resultSet = preparedStatement.getGeneratedKeys();
                if(resultSet.next()) {
                    patient_id = resultSet.getInt(1);
                }
                preparedStatement.close();
                resultSet.close();
        }
        // insert into admitted_patients table
        String insertAdmittedPatients = "INSERT INTO admitted_patients " +
                "VALUES(default, ?, ?, ?, ?)";
        PreparedStatement ps = connection.prepareStatement(insertAdmittedPatients);
        ps.setInt(1, patient_id);
        ps.setString(2, initial_diagnosis);
        ps.setObject(3, arrival_date);
        ps.setObject(4, discharge_date);
        ps.executeUpdate();
        ps.close();
    }
    private int getPrimaryDoctorIdFromLastname(String lastname) throws SQLException
    {
        int employee_id = -1;
        String doctorQuery = "SELECT employee_id FROM employees WHERE lastname = ?";
        PreparedStatement preparedStatement =
                connection.prepareStatement(doctorQuery);
        preparedStatement.setString(1, lastname);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.next()) {
            employee_id = resultSet.getInt("employee_id");
        }
        resultSet.close();
        preparedStatement.close();
        return employee_id;
    }
    private int insertInsurance(String policy_number, String company_name) throws
            SQLException {
        int insurance_id = -1;
        String insuranceQuery = "INSERT INTO insurance VALUES (default, ?, ?)";
        PreparedStatement preparedStatement =
                connection.prepareStatement(insuranceQuery, Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, policy_number);
        preparedStatement.setString(2, company_name);
        preparedStatement.executeUpdate();
        ResultSet resultSet = preparedStatement.getGeneratedKeys();
        if(resultSet.next()) {
            insurance_id = resultSet.getInt(1);
        }
        resultSet.close();
        preparedStatement.close();
        return insurance_id;
    }
    private int insertEmergencyContact(String name, String phone) throws
            SQLException {
        int emergency_contact_id = -1;
        String insuranceQuery = "INSERT INTO emergency_contacts VALUES (default, ?, ?)";
        PreparedStatement preparedStatement =
                connection.prepareStatement(insuranceQuery, Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, name);
        preparedStatement.setString(2, phone);
        preparedStatement.executeUpdate();
        ResultSet resultSet = preparedStatement.getGeneratedKeys();
        if(resultSet.next()) {
            emergency_contact_id = resultSet.getInt(1);
        }
        resultSet.close();
        preparedStatement.close();
        return emergency_contact_id;
    }
    private int checkIfPatientExists(String lastname) throws SQLException {
        int patient_id = -1;
        String checkEmployee = "SELECT * FROM patients WHERE lastname = ?";
        PreparedStatement preparedStatement =
                connection.prepareStatement(checkEmployee);
        preparedStatement.setString(1, lastname);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.next()) {
            patient_id = resultSet.getInt("patient_id");
        }
        return patient_id;
    }
    public DatabaseDriver() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(url, username, password);
    }
    private static DatabaseDriver getInstance() throws SQLException, ClassNotFoundException {
        if(instance == null) {
            instance = new DatabaseDriver();
        }
        return instance;
    }
}