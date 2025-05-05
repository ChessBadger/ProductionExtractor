using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;       // For ZIP extraction
using System.Linq;
using System.Text.RegularExpressions;
using DotNetDBF;                  // DotNetDBF NuGet
using OfficeOpenXml;              // EPPlus NuGet

class Program
{
    static void Main()
    {
        // Get user’s home directory
        string userProfilePath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        string folderPath = Path.Combine(userProfilePath,
                                         "BADGER INVENTORY SERVICE, INC",
                                         "BIS - DATABASE",
                                         "ProductionReports");
        string errorLogFile = Path.Combine(folderPath, "errors.txt");

        var zipFiles = Directory.GetFiles(folderPath, "*.zip");
        if (zipFiles.Length == 0)
        {
            Console.WriteLine("No ZIP files found.");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        foreach (var zipFilePath in zipFiles)
        {
            string zipFileName = Path.GetFileName(zipFilePath);
            string firstFive = zipFileName.Length >= 5
                ? zipFileName.Substring(0, 5)
                : "";

            // Skip/delete filter
            if ((firstFive.Equals("0002-", StringComparison.OrdinalIgnoreCase) ||
                 firstFive.Equals("5001-", StringComparison.OrdinalIgnoreCase)) &&
                 zipFileName.IndexOf("rx", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                Console.WriteLine($"Skipping & deleting: {zipFileName}");
                TryDelete(zipFilePath);
                continue;
            }

            Console.WriteLine($"Processing: {zipFileName}");

            // 1) extract three DBFs into a temp folder
            string tempFolder = Path.Combine(Path.GetTempPath(),
                                             "DBFExtraction",
                                             Path.GetFileNameWithoutExtension(zipFilePath));
            if (Directory.Exists(tempFolder))
                Directory.Delete(tempFolder, true);
            Directory.CreateDirectory(tempFolder);

            string finalDbfPath = ExtractDBF(zipFilePath, "final.dbf", tempFolder);
            string employeeDbfPath = ExtractDBF(zipFilePath, "employee.dbf", tempFolder);
            string todayDbfPath = ExtractDBF(zipFilePath, "today.dbf", tempFolder);

            if (finalDbfPath == null || employeeDbfPath == null || todayDbfPath == null)
            {
                Console.WriteLine("One or more DBF files missing; skipping.");
                continue;
            }

            // 2) read into DataTables
            DataTable finalTbl = ReadDBF(finalDbfPath);
            DataTable employeeTbl = ReadDBF(employeeDbfPath);
            DataTable todayTbl = ReadDBF(todayDbfPath);

            // pull inv_date & store_num from today.dbf
            string invDate = todayTbl.Rows[0][0].ToString();
            string storeNum = todayTbl.Rows[0][6].ToString();

            // Kelley-specific filename overrides
            if (zipFileName.IndexOf("Kelley", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                if (zipFileName.Length >= 9)
                    storeNum = zipFileName.Substring(0, 9).Replace("-", "");
                var m = Regex.Match(zipFileName, @"\d{6}");
                if (m.Success &&
                    DateTime.TryParseExact(m.Value,
                                           new[] { "yyMMdd" },
                                           CultureInfo.InvariantCulture,
                                           DateTimeStyles.None,
                                           out DateTime pd))
                {
                    invDate = pd.ToString("MM/dd/yy");
                }
            }

            // 3) build EmpId → names map
            var employeeMap = employeeTbl.AsEnumerable()
                .GroupBy(r => r[0].ToString())
                .ToDictionary(
                    g => g.Key,
                    g => (LastName: g.Last()[1].ToString(),
                          FirstName: g.Last()[2].ToString())
                );

            // 4) enrich & compute deltas (including new 10/15‑min gaps)
            var enriched = ProcessAndEnrichData(finalTbl, employeeMap, invDate, storeNum);

            // 5) write Excel
            string outName = Path.GetFileNameWithoutExtension(zipFilePath) + ".XLSX";
            string outPath = Path.Combine(folderPath, outName);
            WriteToExcel(outPath, enriched, zipFilePath, errorLogFile);

            // 6) if success, delete ZIP
            if (File.Exists(outPath))
                TryDelete(zipFilePath);
        }

        Console.WriteLine("Done. Press any key to exit...");
        Console.ReadKey();
    }

    static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
            Console.WriteLine($"Deleted: {path}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Delete failed: {ex.Message}");
        }
    }

    static string ExtractDBF(string zip, string target, string destFolder)
    {
        using (var archive = ZipFile.OpenRead(zip))
        {
            var entry = archive.Entries
                                 .FirstOrDefault(e => e.FullName
                                         .Equals(target, StringComparison.OrdinalIgnoreCase));
            if (entry == null) return null;
            string outPath = Path.Combine(destFolder, entry.Name);
            entry.ExtractToFile(outPath, overwrite: true);
            Console.WriteLine($"Extracted {target}");
            return outPath;
        }
    }

    static DataTable ReadDBF(string path)
    {
        var table = new DataTable();
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
        using (var reader = new DBFReader(fs))
        {
            foreach (var fld in reader.Fields)
                table.Columns.Add(fld.Name);

            object[] rowVals;
            while ((rowVals = reader.NextRecord()) != null)
                table.Rows.Add(rowVals);
        }
        return table;
    }

    // Parses your "time" column into DateTime? 
    static DateTime? ParseTime(object timeObj)
    {
        if (timeObj == null || timeObj == DBNull.Value) return null;
        if (timeObj is DateTime dt) return dt;
        if (DateTime.TryParse(timeObj.ToString(), out var parsed))
            return parsed;
        return null;
    }

    static List<dynamic> ProcessAndEnrichData(
        DataTable dbfTable,
        Dictionary<string, (string LastName, string FirstName)> employeeMap,
        string invDate,
        string storeNum)
    {
        // parse invDate once
        DateTime parsedInvDate = DateTime.TryParse(invDate, out var dt)
            ? dt
            : DateTime.MinValue;

        var results = dbfTable.AsEnumerable()
            .Where(r => !string.IsNullOrWhiteSpace(r["employee"].ToString()))
            .GroupBy(r => r["employee"].ToString())
            .Select(g =>
            {
                // 1) sort by "time"
                var times = g
                    .Select(r => ParseTime(r["time"]))
                    .Where(t => t.HasValue)
                    .Select(t => t.Value)
                    .OrderBy(t => t)
                    .ToList();

                // 2) compute intervals
                var intervals = new List<TimeSpan>();
                for (int i = 1; i < times.Count; i++)
                    intervals.Add(times[i] - times[i - 1]);

                // 3) avg delta in minutes
                double avgDelta = intervals.Any()
                    ? intervals.Average(d => d.TotalMinutes)
                    : 0.0;

                // 4) gap counts for ≥5, ≥10, ≥15 minutes
                int gap5 = intervals.Count(d => d.TotalMinutes >= 5);
                int gap10 = intervals.Count(d => d.TotalMinutes >= 10);
                int gap15 = intervals.Count(d => d.TotalMinutes >= 15);

                // existing aggregates
                int countRec = g.Count();
                decimal totalQty = g.Sum(r => Convert.ToDecimal(r["units"]) *
                                               Convert.ToDecimal(r["quantity2"]));
                decimal totalPrice = g.Sum(r => Convert.ToDecimal(r["price"]) *
                                               Convert.ToDecimal(r["units"]) *
                                               Convert.ToDecimal(r["quantity2"]));

                string empId = g.Key;
                var nameTuple = employeeMap.ContainsKey(empId)
                                     ? employeeMap[empId]
                                     : (LastName: "", FirstName: "");
                string lastSerial = g.Last()["serial"].ToString();

                return new
                {
                    Employee = empId,
                    CountRecord = countRec,
                    TotalExtQty = totalQty,
                    TotalExtPrice = totalPrice,
                    EmpId = empId,
                    LastName = nameTuple.LastName,
                    FirstName = nameTuple.FirstName,
                    InvDate = parsedInvDate,
                    StoreNum = storeNum,
                    LastSerial = lastSerial,
                    AvgDelta = avgDelta,
                    GapCount = gap5,
                    Gap10Count = gap10,
                    Gap15Count = gap15
                };
            })
            .ToList<dynamic>();

        return results;
    }

    static void WriteToExcel(
        string filePath,
        List<dynamic> data,
        string zipFilePath,
        string errorLogFile)
    {
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        using (var p = new ExcelPackage())
        {
            var ws = p.Workbook.Worksheets.Add("EMP_RPT");

            // --- Headers ---
            string[] headers = {
                "Employee","Count_Record","Total_Ext_Qty","Total_Ext_Price",
                "EMP_ID","LAST_NAME","FIRST_NAME","MID_INIT","SEC_NAME",
                "SSN","STATUS","RATE","HOURS","INV_DATE","TIME_IN",
                "TIME_OUT","LAST_INV_DATE","TEAM_LEADER","NO_TEAM",
                "STORE_NUM","SERIAL","AVG_DELTA","GAP5_COUNT",
                "GAP10_COUNT","GAP15_COUNT"
            };
            for (int i = 0; i < headers.Length; i++)
                ws.Cells[1, i + 1].Value = headers[i];

            // --- Data rows ---
            int row = 2;
            foreach (var x in data)
            {
                ws.Cells[row, 1].Value = x.Employee;
                ws.Cells[row, 2].Value = x.CountRecord;
                ws.Cells[row, 3].Value = x.TotalExtQty;
                ws.Cells[row, 4].Value = x.TotalExtPrice;
                ws.Cells[row, 5].Value = x.EmpId;
                ws.Cells[row, 6].Value = x.LastName;
                ws.Cells[row, 7].Value = x.FirstName;
                ws.Cells[row, 14].Value = x.InvDate;
                ws.Cells[row, 20].Value = x.StoreNum;
                ws.Cells[row, 21].Value = x.LastSerial;
                ws.Cells[row, 22].Value = x.AvgDelta;
                ws.Cells[row, 23].Value = x.GapCount;
                ws.Cells[row, 24].Value = x.Gap10Count;
                ws.Cells[row, 25].Value = x.Gap15Count;
                row++;
            }

            if (row == 2)
            {
                // no data → log
                LogError(zipFilePath, errorLogFile);
            }
            else
            {
                // formatting
                ws.Cells[2, 3, row - 1, 3].Style.Numberformat.Format = "#,##0.0000";
                ws.Cells[2, 4, row - 1, 4].Style.Numberformat.Format = "#,##0.0000";
                ws.Cells[2, 14, row - 1, 14].Style.Numberformat.Format = "mm/dd/yy";
                ws.Cells[2, 22, row - 1, 22].Style.Numberformat.Format = "0.00";
                ws.Cells[2, 23, row - 1, 23].Style.Numberformat.Format = "0";
                ws.Cells[2, 24, row - 1, 24].Style.Numberformat.Format = "0";
                ws.Cells[2, 25, row - 1, 25].Style.Numberformat.Format = "0";

                ws.Cells[ws.Dimension.Address].AutoFitColumns();
            }

            p.SaveAs(new FileInfo(filePath));
        }
    }

    static void LogError(string zipFilePath, string errorLogFile)
    {
        try
        {
            string msg = $"Zip File Is Blank: {Path.GetFileName(zipFilePath)}";
            File.AppendAllText(errorLogFile, msg + Environment.NewLine);
            Console.WriteLine($"Logged blank output for {Path.GetFileName(zipFilePath)}.");

            // open errors.txt in Notepad
            Process.Start(new ProcessStartInfo
            {
                FileName = "notepad.exe",
                Arguments = errorLogFile,
                UseShellExecute = true
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to log error: {ex.Message}");
        }
    }
}
