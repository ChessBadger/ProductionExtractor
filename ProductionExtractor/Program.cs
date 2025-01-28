using System;
using System.Collections.Generic;
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

        // Get the user's home directory dynamically
        //string userProfilePath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        // Define the dynamic folder path
        //string folderPath = Path.Combine(userProfilePath, "BADGER INVENTORY SERVICE, INC", "BIS - ProductionReports");

        string folderPath = "C:\\Users\\Laptop 122\\Desktop\\Store Prep\\Production extraction project";

        // Get all ZIP files in the folder
        var zipFiles = Directory.GetFiles(folderPath, "*.zip");
        if (zipFiles.Length == 0)
        {
            Console.WriteLine("No ZIP files found in the specified folder.");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        // Process each ZIP file individually
        foreach (var zipFilePath in zipFiles)
        {
            Console.WriteLine($"Processing ZIP file: {Path.GetFileName(zipFilePath)}");

            // Create a temp folder for extracting files
            string tempFolder = Path.Combine(Path.GetTempPath(), "DBFExtraction", Path.GetFileNameWithoutExtension(zipFilePath));
            if (Directory.Exists(tempFolder))
                Directory.Delete(tempFolder, true);
            Directory.CreateDirectory(tempFolder);

            // Extract and load final.dbf
            string finalDbfPath = ExtractSpecificDBFFromZip(zipFilePath, "final.dbf", tempFolder);
            if (finalDbfPath == null)
            {
                Console.WriteLine("final.dbf not found in the ZIP file.");
                continue; // Skip to the next ZIP file
            }
            DataTable finalDbfTable = ReadDBF(finalDbfPath);

            // Extract and load employee.dbf
            string employeeDbfPath = ExtractSpecificDBFFromZip(zipFilePath, "employee.dbf", tempFolder);
            if (employeeDbfPath == null)
            {
                Console.WriteLine("employee.dbf not found in the ZIP file.");
                continue; // Skip to the next ZIP file
            }
            DataTable employeeDbfTable = ReadDBF(employeeDbfPath);

            // Extract and load today.dbf
            string todayDbfPath = ExtractSpecificDBFFromZip(zipFilePath, "today.dbf", tempFolder);
            if (todayDbfPath == null)
            {
                Console.WriteLine("today.dbf not found in the ZIP file.");
                continue; // Skip to the next ZIP file
            }
            DataTable todayDbfTable = ReadDBF(todayDbfPath);

            // Extract the inv_date and store_num from the first row of today.dbf
            string invDate = todayDbfTable.Rows[0][0].ToString();  // First column (inv_date)
            string storeNum = todayDbfTable.Rows[0][6].ToString(); // Seventh column (store_num)

            // Extract the last 3 digits of the store_num
            string storeSuffix = storeNum.Length >= 3 ? storeNum.Substring(storeNum.Length - 3) : storeNum;

            // Create the dynamic filename
            //string outputFilePath = Path.Combine(folderPath, $"BIS{storeSuffix}.xlsx");

            // Create the dynamic filename based on the ZIP file name
            string outputFileName = Path.GetFileNameWithoutExtension(zipFilePath) + ".xlsx";
            string outputFilePath = Path.Combine(folderPath, outputFileName);


            // Create a mapping of EmpId -> (Last_Name, First_Name)
            var employeeMap = employeeDbfTable.AsEnumerable()
    .GroupBy(row => row[0].ToString()) // Group by EmpId
    .ToDictionary(
        g => g.Key, // Use the key from the group
        g => (dynamic)new
        {
            LastName = g.Last()[1].ToString(),  // Take the last entry for Last_Name
            FirstName = g.Last()[2].ToString() // Take the last entry for First_Name
        });


            // Process final.dbf and add inv_date and store_num
            var results = ProcessAndEnrichData(finalDbfTable, employeeMap, invDate, storeNum);

            // Write enriched data to Excel
            WriteToExcel(outputFilePath, results);

            Console.WriteLine($"Data successfully extracted and written to {outputFilePath}.");
        }

        Console.WriteLine("All ZIP files processed.");
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    static string ExtractSpecificDBFFromZip(string zipFilePath, string targetDbfFileName, string tempFolder)
    {
        string dbfFilePath = null;

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

    static List<dynamic> ProcessAndEnrichData(DataTable dbfTable, Dictionary<string, dynamic> employeeMap, string invDate, string storeNum)
    {
        // Parse invDate as DateTime
        DateTime parsedInvDate;
        if (!DateTime.TryParse(invDate, out parsedInvDate))
        {
            // If parsing fails, assign a default value (e.g., DateTime.MinValue)
            parsedInvDate = DateTime.MinValue;
        }

        var results = dbfTable.AsEnumerable()
            .Where(row => !string.IsNullOrWhiteSpace(row["employee"].ToString())) // Ignore blank employee values
            .GroupBy(row => row["employee"].ToString())
            .Select(g => new
            {
                Employee = g.Key,
                CountRecord = g.Count(),
                TotalExtQty = g.Sum(r => Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                TotalExtPrice = g.Sum(r => Convert.ToDecimal(r["price"]) * Convert.ToDecimal(r["units"]) * Convert.ToDecimal(r["quantity2"])),
                EmpId = g.Key,
                LastName = employeeMap.ContainsKey(g.Key) ? employeeMap[g.Key].LastName : "Unknown",
                FirstName = employeeMap.ContainsKey(g.Key) ? employeeMap[g.Key].FirstName : "Unknown",
                InvDate = parsedInvDate, // Store as DateTime
                StoreNum = storeNum
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
            worksheet.Cells[1, 2].Value = "Count_Record";
            worksheet.Cells[1, 3].Value = "Total_Ext_Qty";
            worksheet.Cells[1, 4].Value = "Total_Ext_Price";
            worksheet.Cells[1, 5].Value = "EMP_ID";
            worksheet.Cells[1, 6].Value = "LAST_NAME";
            worksheet.Cells[1, 7].Value = "FIRST_NAME";
            worksheet.Cells[1, 14].Value = "INV_DATE";
            worksheet.Cells[1, 20].Value = "STORE_NUM";

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
                worksheet.Cells[row, 14].Value = item.InvDate;
                worksheet.Cells[row, 20].Value = item.StoreNum;

                row++;
            }

            // Only apply formatting if there is data in the worksheet
            if (row > 2) // Check if data rows were added (row = 2 means no data)
            {
                // Format specific columns
                worksheet.Cells[2, 3, row - 1, 3].Style.Numberformat.Format = "#,##0.0000"; // Format TotalExtQty
                worksheet.Cells[2, 4, row - 1, 4].Style.Numberformat.Format = "#,##0.0000"; // Format TotalExtPrice
                worksheet.Cells[2, 14, row - 1, 14].Style.Numberformat.Format = "mm/dd/yy"; // Format InvDate as mm/dd/yy

                // Auto-fit columns for better readability
                worksheet.Cells[worksheet.Dimension.Address].AutoFitColumns();
            }

            // Save file
            package.SaveAs(new FileInfo(filePath));
        }
    }
}