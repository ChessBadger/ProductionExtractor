﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.IO.Compression; // For working with zip files
using System.Linq;
using DotNetDBF; // Install DotNetDBF NuGet package
using OfficeOpenXml; // Install EPPlus NuGet package

class Program
{
    static void Main()
    {
        string folderPath = "C:\\Users\\Laptop 122\\Desktop\\Store Prep\\Production extraction project"; // Path to search for ZIP files
        string outputFilePath = "C:\\Users\\Laptop 122\\Desktop\\Store Prep\\Production extraction project\\output.xlsx";

        // Extract and load final.dbf
        string finalDbfPath = ExtractSpecificDBFFromAnyZip(folderPath, "final.dbf");
        if (finalDbfPath == null)
        {
            Console.WriteLine("final.dbf not found in any ZIP file in the specified folder.");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        DataTable finalDbfTable = ReadDBF(finalDbfPath);

        // Extract and load employee.dbf
        string employeeDbfPath = ExtractSpecificDBFFromAnyZip(folderPath, "employee.dbf");
        if (employeeDbfPath == null)
        {
            Console.WriteLine("employee.dbf not found in any ZIP file in the specified folder.");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        DataTable employeeDbfTable = ReadDBF(employeeDbfPath);

        // Create a mapping of EmpId -> (Last_Name, First_Name)
        var employeeMap = employeeDbfTable.AsEnumerable()
            .ToDictionary(
                row => row[0].ToString(), // EmpId is in the first column
                row => (dynamic)new
                {
                    LastName = row[1].ToString(),  // Last_Name is in the second column
                    FirstName = row[2].ToString() // First_Name is in the third column
                });

        // Process final.dbf and join with employee.dbf data
        var results = ProcessAndEnrichData(finalDbfTable, employeeMap);

        // Write enriched data to Excel
        WriteToExcel(outputFilePath, results);

        Console.WriteLine($"Data successfully extracted and written to {outputFilePath}.");
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    static string ExtractSpecificDBFFromAnyZip(string folderPath, string targetDbfFileName)
    {
        string tempFolder = Path.Combine(Path.GetTempPath(), "DBFExtraction");

        // Ensure the temporary folder exists
        if (!Directory.Exists(tempFolder))
            Directory.CreateDirectory(tempFolder);

        string dbfFilePath = null;

        // Search for ZIP files in the directory
        var zipFiles = Directory.GetFiles(folderPath, "*.zip");
        if (zipFiles.Length == 0)
        {
            Console.WriteLine("No ZIP files found in the specified folder.");
            return null;
        }

        foreach (var zipFilePath in zipFiles)
        {
            using (ZipArchive archive = ZipFile.OpenRead(zipFilePath))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    if (entry.FullName.Equals(targetDbfFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        // Extract the target DBF file to the temp folder
                        dbfFilePath = Path.Combine(tempFolder, entry.Name);
                        entry.ExtractToFile(dbfFilePath, overwrite: true);
                        Console.WriteLine($"Extracted {entry.FullName} from {Path.GetFileName(zipFilePath)}");
                        return dbfFilePath; // Return once the target file is found and extracted
                    }
                }
            }
        }

        return dbfFilePath; // Null if the file is not found
    }

    static DataTable ReadDBF(string filePath)
    {
        DataTable table = new DataTable();

        // Open the DBF file as a FileStream
        using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        {
            using (var reader = new DBFReader(fileStream))
            {
                // Get column definitions
                var fields = reader.Fields;
                foreach (var field in fields)
                {
                    table.Columns.Add(field.Name);
                }

                // Read rows
                object[] rowValues;
                while ((rowValues = reader.NextRecord()) != null)
                {
                    table.Rows.Add(rowValues);
                }
            }
        }

        return table;
    }

    static List<dynamic> ProcessAndEnrichData(DataTable dbfTable, Dictionary<string, dynamic> employeeMap)
    {
        var results = dbfTable.AsEnumerable()
            .GroupBy(row => row["employee"].ToString())
            .Select(g => new
            {
                Employee = g.Key,
                CountRecord = g.Count(),
                TotalExtQty = g.Sum(r => Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                TotalExtPrice = g.Sum(r => Convert.ToDecimal(r["price"]) * Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                EmpId = g.Key,
                LastName = employeeMap.ContainsKey(g.Key) ? employeeMap[g.Key].LastName : "Unknown",
                FirstName = employeeMap.ContainsKey(g.Key) ? employeeMap[g.Key].FirstName : "Unknown"
            })
            .ToList<dynamic>();

        return results;
    }

    static void WriteToExcel(string filePath, List<dynamic> data)
    {
        OfficeOpenXml.ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;

        using (var package = new ExcelPackage())
        {
            var worksheet = package.Workbook.Worksheets.Add("Employee Summary");

            // Add headers
            worksheet.Cells[1, 1].Value = "Employee";
            worksheet.Cells[1, 2].Value = "CountRecord";
            worksheet.Cells[1, 3].Value = "TotalExtQty";
            worksheet.Cells[1, 4].Value = "TotalExtPrice";
            worksheet.Cells[1, 5].Value = "EmpId";
            worksheet.Cells[1, 6].Value = "Last_Name";
            worksheet.Cells[1, 7].Value = "First_Name";

            // Add data
            int row = 2;
            foreach (var item in data)
            {
                worksheet.Cells[row, 1].Value = item.Employee;
                worksheet.Cells[row, 2].Value = item.CountRecord;
                worksheet.Cells[row, 3].Value = item.TotalExtQty;
                worksheet.Cells[row, 4].Value = item.TotalExtPrice;
                worksheet.Cells[row, 5].Value = item.EmpId;
                worksheet.Cells[row, 6].Value = item.LastName;
                worksheet.Cells[row, 7].Value = item.FirstName;
                row++;
            }

            // Save file
            package.SaveAs(new FileInfo(filePath));
        }
    }
}
