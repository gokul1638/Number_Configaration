package com.rms.automation.export;

import com.rms.automation.service.*;
import java.util.*;

public class DataExporter {

	
	  private static final String[] RMS_TABLES = {
	            "PAIRED_INFO_TABLE",
	            "device_sim_table",
	            "customer_attribute",
	            "device_msisdn_table"
	    };

	    @SuppressWarnings("resource")
		public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        // Step 1: Input MSISDNs
        System.out.println("Enter MSISDNs separated by comma:");
        String input = scanner.nextLine();
        String[] msisdns = input.split(",");
        for (int i = 0; i < msisdns.length; i++) {
            msisdns[i] = msisdns[i].trim();
        }

        // Step 2: Ask RMS status
        System.out.println("Enter RMS status to export (Active / Inactive):");
        String rmsStatusInput = scanner.nextLine().trim();
        String rmsStatusValue = rmsStatusInput.equalsIgnoreCase("Active") ? "1" : "0";

        // Step 3: Ask CIM status
        System.out.println("Enter CIM status to export (Active / Initial / Expired):");
        String cimStatus = scanner.nextLine().trim();

        // Step 4: Confirm
        System.out.println("You selected RMS status = " + rmsStatusInput + " (" + rmsStatusValue + "), CIM status = " + cimStatus);
        System.out.println("Proceed with export? (yes/no):");
        String confirm = scanner.nextLine();
        
        if (!confirm.equalsIgnoreCase("yes")) {
            System.out.println("❌ Export cancelled.");
            return ;
            
        }// test here
        
        // Step 5: Export RMS data
        for (String table : RMS_TABLES) {
        	ExportService.exportRMSTable(table, msisdns, rmsStatusValue, cimStatus);
        }

        // Step 6: Export CIM data
        ExportService.exportCIMTables(msisdns, cimStatus);

        // Step 7: Export CIM Catalog Master Table
        ExportService.exportCIMCatalog(msisdns, cimStatus);
        
       //Step 8: Now perform deletion from PREPOD
        System.out.println("\n Now starting cleanup in PREPOD environment...");
        com.rms.automation.service.DeleteService.deletePrepodData(msisdns);

        
        //step9 insert rms
        RMSInsertService.insertRMSTablesFromSQL("C:\\Users\\Kishore Kumar\\eclipse-workspace\\RMSAutomation/");

        scanner.close();
    }
}

    