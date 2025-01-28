using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using DotNetDBF; // Install DotNetDBF NuGet package
using OfficeOpenXml; // Install EPPlus NuGet package

class Program
{
    static void Main()
    {
        string inputFilePath = "C:\\Users\\Laptop 122\\Desktop\\Store Prep\\final.dbf";
        string outputFilePath = "C:\\Users\\Laptop 122\\Desktop\\Store Prep\\output.xlsx";

        // Read DBF file
        DataTable dbfTable = ReadDBF(inputFilePath);

        // Process data
        var results = ProcessData(dbfTable);

        // Write to Excel
        WriteToExcel(outputFilePath, results);
    }

    static DataTable ReadDBF(string filePath)
    {
        DataTable table = new DataTable();

        // Open the DBF file as a FileStream
        using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        {
            using (var reader = new DBFReader(fileStream)) // Pass the FileStream instead of a string
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


    static List<dynamic> ProcessData(DataTable dbfTable)
    {
        var results = dbfTable.AsEnumerable()
            .GroupBy(row => row["employee"].ToString())
            .Select(g => new
            {
                Employee = g.Key,
                CountRecord = g.Count(),
                TotalExtQty = g.Sum(r => Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                TotalExtPrice = g.Sum(r => Convert.ToDecimal(r["price"]) * Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                EmpId = g.Key
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

            // Add data
            int row = 2;
            foreach (var item in data)
            {
                worksheet.Cells[row, 1].Value = item.Employee;
                worksheet.Cells[row, 2].Value = item.CountRecord;
                worksheet.Cells[row, 3].Value = item.TotalExtQty;
                worksheet.Cells[row, 4].Value = item.TotalExtPrice;
                worksheet.Cells[row, 5].Value = item.EmpId;
                row++;
            }

            // Save file
            package.SaveAs(new FileInfo(filePath));
        }
    }
}
