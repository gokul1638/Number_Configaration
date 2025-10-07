package com.rms.automation.delete;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import com.rms.automation.util.DBConnection;

import java.nio.file.*;

public class DataCleaner {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        System.out.print("Enter the export folder path where .sql files are stored: ");
        String folderPath = sc.nextLine().trim();

        // ✅ Step 1: Extract MSISDNs and Device IDs
        List<String> msisdns = extractValuesFromSQL(folderPath, "MSISDN");
        List<String> deviceIds = extractValuesFromSQL(folderPath, "device_id");

        System.out.println("\n Extracted data from SQL files:");
        System.out.println("MSISDNs: " + msisdns);
        System.out.println("Device IDs: " + deviceIds);

        System.out.print("\nProceed to check and delete from prepod? (yes/no): ");
        String proceed = sc.nextLine();

        if (!proceed.equalsIgnoreCase("yes")) {
            System.out.println("❌ Operation cancelled.");
            sc.close();
            return;
        }

        // ✅ Step 2: Delete from Prepod RMS
        deleteFromPrepodRMS(deviceIds, msisdns);

        // ✅ Step 3: Delete from Prepod CIM (4 schemas)
        deleteFromPrepodCIM(msisdns);

        System.out.println("\n✅ Cleanup complete. Ready for insertion next.");
        sc.close();
    }

    // -------------------- Utility: Extract MSISDNs or Device IDs from SQL --------------------
    private static List<String> extractValuesFromSQL(String folderPath, String columnName) {
        List<String> values = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\('?(\\d{7,15})'?\\)");

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folderPath), "*.sql")) {
            for (Path file : stream) {
                List<String> lines = Files.readAllLines(file);
                for (String line : lines) {
                    if (line.contains(columnName)) {
                        Matcher matcher = pattern.matcher(line);
                        while (matcher.find()) {
                            values.add(matcher.group(1));
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading SQL files: " + e.getMessage());
        }

        return values;
    }

    // -------------------- Delete from Prepod RMS --------------------
    private static void deleteFromPrepodRMS(List<String> deviceIds, List<String> msisdns) {
        try (Connection conn = DBConnection.getRMSConnection()) {
            System.out.println("\n🔍 Checking existing records in prepod_rms...");

            // device_msisdn_table
            showAndDelete(conn, "prepod_rms.device_msisdn_table", "device_id", deviceIds);

            // paired_info_table
            showAndDelete(conn, "prepod_rms.paired_info_table", "PAIRED_DEVICE_ID", deviceIds);

            // customer_attribute
            showAndDelete(conn, "prepod_rms.customer_attribute", "MSISDN", msisdns);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // -------------------- Delete from Prepod CIM --------------------
    private static void deleteFromPrepodCIM(List<String> msisdns) {
        String[] cimSchemas = {
            "prepod_user125",
            "prepod_user126",
            "prepod_user127",
            "prepod_user128"
        };

        try (Connection conn = DBConnection.getCIMConnection()) {

            System.out.println("\n🔍 Checking existing records in prepod CIM schemas...");

            for (String schema : cimSchemas) {
                System.out.println("\n🟡 Checking schema: " + schema);

                showAndDelete(conn, schema + ".cim_sgmnt_catalog", "cim_subs_msisdn", msisdns);
                showAndDelete(conn, schema + ".cim_acnt_subscrptn_dtl", "msisdn", msisdns);
                showAndDelete(conn, schema + ".customer_attribute", "msisdn", msisdns);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // -------------------- Common delete method --------------------
    private static void showAndDelete(Connection conn, String tableName, String columnName, List<String> values) {
        if (values.isEmpty()) return;

        String inClause = String.join(",", Collections.nCopies(values.size(), "?"));
        String selectQuery = "SELECT * FROM " + tableName + " WHERE " + columnName + " IN (" + inClause + ")";
        String deleteQuery = "DELETE FROM " + tableName + " WHERE " + columnName + " IN (" + inClause + ")";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
            for (int i = 0; i < values.size(); i++) selectStmt.setString(i + 1, values.get(i));

            ResultSet rs = selectStmt.executeQuery();
            boolean found = false;

            while (rs.next()) {
                if (!found) System.out.println("🟢 Found records in " + tableName + ":");
                found = true;
                System.out.println(" → " + columnName + ": " + rs.getString(columnName));
            }

            if (found) {
                System.out.print("⚠️ Do you want to delete from " + tableName + "? (yes/no): ");
                Scanner sc = new Scanner(System.in);
                String ans = sc.nextLine();
                if (ans.equalsIgnoreCase("yes")) {
                    try (PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery)) {
                        for (int i = 0; i < values.size(); i++) deleteStmt.setString(i + 1, values.get(i));
                        int deleted = deleteStmt.executeUpdate();
                        System.out.println("🗑️ Deleted " + deleted + " record(s) from " + tableName);
                    }
                } else {
                    System.out.println("⏩ Skipped deletion for " + tableName);
                }
            } else {
                System.out.println("⚪ No records found in " + tableName);
            }

        } catch (SQLException e) {
            System.err.println("Error processing " + tableName + ": " + e.getMessage());
        }
    }
}

