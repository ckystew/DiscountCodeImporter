using ExcelDataReader;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UniqueCustomerDiscountImporter
{
    public class Program
    {
        static string connectionString = ""; // needs pointing at database
        static int processedRecords = 0;
        static int erroredRecords = 0;
        static int lastDiscountId;

        public static void Main(string[] args)
        {
            DataSet results;
            lastDiscountId = SaveLastDiscountCodeId();

            Console.WriteLine("Enter file location (e.g. 'C:/Temp/sheet.xls')");
            var fileLocation = Console.ReadLine();
            
            Console.Write("Finding spreadsheet...");
            FileStream stream = File.Open(fileLocation, FileMode.Open, FileAccess.Read);
            Console.WriteLine("..spreadsheet found!");

            Console.Write("Importing spreadsheet...");
            using (var reader = ExcelReaderFactory.CreateReader(stream))
            {
                do
                {
                    while (reader.Read())
                    {
                    }
                }
                while (reader.NextResult());

                results = reader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (tableReader) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });
            }
            Console.WriteLine("..spreadsheet imported!");

            Console.WriteLine("Specify the name of the header that contains the UserIds:");
            var userIdHeaderName = Console.ReadLine();
            Console.WriteLine("Specify the the name of the header that contains the Discount Codes:");
            var codeHeadername = Console.ReadLine();
            
            Console.WriteLine("Importing...");
            foreach(DataTable table in results.Tables)
            {
                foreach(DataRow row in table.Rows)
                {
                    var entry = PopulateDefaultCodeValues();
                    entry.CustomerId = int.Parse(row[userIdHeaderName].ToString());
                    entry.Code = row[codeHeadername].ToString();

                    Console.WriteLine("{0} {1}", entry.CustomerId, entry.Code);

                    SaveEntryToDatabase(entry);
                }
            }

            Console.WriteLine("Processing Complete!");
            Console.WriteLine(processedRecords + " records saved");
            Console.WriteLine(erroredRecords + " records errored");

            if (lastDiscountId != -1)
            {
                Console.WriteLine("Would you like the discount codes to only work against particular services? (Y/N)");
                if (Console.ReadLine() == "Y")
                {
                    ProcessDiscountCodeServices();
                }
            }

            Console.ReadLine();
        }

        private static int SaveLastDiscountCodeId()
        {
            int discountCodeId = -1;
            var queryString = "SELECT TOP (1) DiscountCodeId FROM [Customer].[DiscountCode] ORDER BY DiscountCodeId DESC";

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(queryString, conn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            discountCodeId = int.Parse(reader["DiscountCodeId"].ToString());
                        }
                    }
                }
            }
            return discountCodeId;
        }

        private static void ProcessDiscountCodeServices()
        {
            Console.WriteLine("Please enter the service ID's to which these services should apply (Return after each entry). Enter C when complete.");
            bool finished = false;
            List<int> serviceIds = new List<int>();

            while (!finished)
            {
                var entry = Console.ReadLine();
                if (entry == "C")
                {
                    finished = true;
                }
                else
                {
                    try {
                        serviceIds.Add(int.Parse(entry));
                    }
                    catch {
                        Console.WriteLine("Could not parse integer");
                    }
                }
            }

            Console.Write("Collecting discount code ids...");
            List<int> discountCodeIds = CollectDiscountCodeIds();
            Console.WriteLine("..collected!");

            Console.Write("Saving service restrictions to database...");
            foreach(var serviceId in serviceIds)
            {
                foreach (var discountCodeId in discountCodeIds)
                {
                    SaveDiscountCodeServices(serviceId, discountCodeId);
                }
            }
            Console.WriteLine("..complete!");
        }

        private static void SaveDiscountCodeServices(int serviceId, int discountCodeId)
        {
            var queryString = "INSERT INTO Customer.DiscountCodeService (DiscountCodeId, ServiceId) VALUES (" + discountCodeId + ", " + serviceId + ")";
            using (var conn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(queryString, conn))
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static List<int> CollectDiscountCodeIds()
        {
            var ids = new List<int>();
            var queryString = "SELECT DiscountCodeId FROM [Customer].[DiscountCode] WHERE DiscountCodeID > " + lastDiscountId;

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(queryString, conn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            ids.Add(int.Parse(reader["DiscountCodeId"].ToString()));
                        }
                    }
                }
            }
            return ids;
        }

        private static void SaveEntryToDatabase(CustomerDiscountCode entry)
        {
            var queryString = "INSERT INTO Customer.DiscountCode (DiscountCodeTypeID, DiscountCodeGroupID, Code, Description, Amount, Percentage, OverrideLimit, MinimumSpend, CustomerID, Expires, Active, DateAdded, AddedBy, ValidationSProc, WebsiteID, MaximumAmount, ApplicableToDisallowedServices, CheapestParcelOnly, UniqueAddress, IncludeVat, MinWeight, MaxWeight) "
                    + "VALUES(" + entry.DiscountCodeTypeId + ", "
                    + entry.DiscountCodeGroupId + ", "
                    + "'" + entry.Code + "',"
                    + "'" + entry.Description + "', "
                    + entry.Amount + ", "
                    + entry.Percentage + ", "
                    + entry.OverrideLimit + ", "
                    + entry.MinimumSpend + ", "
                    + entry.CustomerId + ", "
                    + "'" + entry.Expires.ToShortDateString() + "', "
                    + entry.Active + ", "
                    + "'" + entry.DateAdded.ToShortDateString() + "', "
                    + "'" + entry.AddedBy + "', "
                    + "NULL, "
                    + entry.WebsiteId + ", "
                    + "NULL, "
                    + entry.ApplicableToDisallowedServices + ", "
                    + entry.CheapestParcelOnly + ", "
                    + entry.UniqueAddress + ", "
                    + entry.IncludeVat + ", "
                    + "NULL, NULL );";

            queryString = queryString.Replace("True", "1").Replace("False", "0");

            try
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    using (var cmd = new SqlCommand(queryString, conn))
                    {
                        conn.Open();
                        cmd.ExecuteNonQuery();
                        processedRecords++;
                    }
                }
            }
            catch
            {
                Console.WriteLine("Error saving. UserId: " + entry.CustomerId);
                erroredRecords++;
            }
        }

        static CustomerDiscountCode PopulateDefaultCodeValues()
        {
            var entry = new CustomerDiscountCode()
            {
                Description = "Valentines 2018",
                Amount = 0,
                Percentage = 20,
                Expires = new DateTime(2018, 3, 1),
                AddedBy = "stew",
                WebsiteId = 1400,

                DiscountCodeTypeId = 2, // 2 = single use
                DiscountCodeGroupId = 3, // 3 generic marketing

                OverrideLimit = false,
                MinimumSpend = 0,
                Active = true,
                DateAdded = DateTime.Now,
                ApplicableToDisallowedServices = false,
                CheapestParcelOnly = false,
                UniqueAddress = false,
                IncludeVat = false
            };

            return entry;
        }


        private class CustomerDiscountCode
        {
            public int Id { get; set; }
            public int DiscountCodeTypeId { get; set; }
            public int DiscountCodeGroupId { get; set; }
            public string Code { get; set; }
            public string Description { get; set; }
            public decimal Amount { get; set; }
            public decimal Percentage { get; set; }
            public bool OverrideLimit { get; set; }
            public decimal MinimumSpend { get; set; }
            public int CustomerId { get; set; }
            public DateTime Expires { get; set; }
            public bool Active { get; set; }
            public DateTime DateAdded { get; set; }
            public string ValidationSProc { get; set; }
            public string AddedBy { get; set; }
            public int WebsiteId { get; set; }
            public bool ApplicableToDisallowedServices { get; set; }
            public bool CheapestParcelOnly { get; set; }
            public bool UniqueAddress { get; set; }
            public bool IncludeVat { get; set; }
            public decimal? MaximumAmount { get; set; }
            public decimal? MinWeight { get; set; }
            public decimal? MaxWeight { get; set; }
        }
    }
}
