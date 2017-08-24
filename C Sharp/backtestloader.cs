using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data;
using System.IO;
using System.Configuration;

namespace BacktestLoader
{
    class Program
    {
        static void Main(string[] args)
        {

            /*
            Console.WriteLine("Make sure to close the Excel file before continuing.");
            Console.Write("Excel file's name: ");         
            string path = Console.ReadLine();
            string ExcelFileName = @"C:\Users\tranda\Desktop\Files\" + path +".xlsx";
            Console.Write("Backtest ID: ");
            string backTestid = Consol.esReadLine();
            Console.Write("Quant. Sub Strategy ID: ");
            string quantSubid = Console.ReadLine();
            Console.Write("Flag: ");
            string actualflag = Console.ReadLine();
            Console.Write("Update ID: ");
            string updateID = Console.ReadLine();
            string updateTime = DateTime.Now.ToShortTimeString();
            */

            /* test inputs */
            //string ExcelFileName = @"C:\Users\tranda\Desktop\Files\Persons.xlsx";
            //string sqlConnectionString = @"Data Source=(localdb)\mylocaldb;Integrated Security=True";


            string ExcelFileName = @"C:\Users\tranda\Desktop\Files\PRAI TW Monthly Update_20170707.xlsx";
            string sqlConnectionString = @"Data Source=(localdb)\mylocaldb;Integrated Security=True";
            string ExcelConnectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Extended Properties=\"Excel 12.0 Xml;HDR=Yes;IMEX=1\";", ExcelFileName);
            
            using (OleDbConnection excelConn = new OleDbConnection(ExcelConnectionString))
            {
                excelConn.Open();
                OleDbCommand sheet2 = new OleDbCommand
                                ("Select * FROM [Daily Stats$A3:H]", excelConn);
            
                // Read data from Excel file
                OleDbDataReader reader2 = sheet2.ExecuteReader();
                                
                // Bulk copy to datebase
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(sqlConnectionString))
                {
                    bulkcopy.DestinationTableName = "dbo.qbd";
                    bulkcopy.ColumnMappings.Add("Date", "BacktestReportDate");
                    bulkcopy.ColumnMappings.Add("AvgLongExp", "LMV");
                    bulkcopy.ColumnMappings.Add("AvgShortExp", "SMV");
                    bulkcopy.ColumnMappings.Add("PnL", "PnL");
                    bulkcopy.ColumnMappings.Add("Return", "ReturnGross");
                    bulkcopy.ColumnMappings.Add("ReturnUnlevered", "ReturnUnlevered");
                    bulkcopy.ColumnMappings.Add("Deal Num", "NumPos");
                    bulkcopy.WriteToServer(reader2);
                }
                excelConn.Close();
                
                /*
                 * Set backtestID, QuantSubStrategyID, ActualFlag, updateID, and UpdateDateTime.
                 */
                using (SqlConnection connection = new SqlConnection(sqlConnectionString))
                {
                    connection.Open();
                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = @"UPDATE qbd SET backtestID = 1, QuantSubStrategyID = 1, 
                                            ActualFlag = 0, updateID ='daniel', UpdateDateTime = GETDATE()";
                        cmd.ExecuteNonQuery();
                    }
                    connection.Close();
                }
            }
            Console.WriteLine("Done..");
            Console.ReadLine();
        }
    }
}
    //class Program
    //{
    //    static void Main(string[] args)
    //    {
    //        // load data source 
    //        string connectionString = @"Data Source=(localdb)\mylocaldb;Integrated Security=True";
    //        string path = @"C:\Users\tranda\Documents\Visual Studio 2015\Projects\BacktestLoader\command.txt";

    //        // needs to delete pre-existing objects and constrains first
    //        makeTable(connectionString,path);
            
    //        Console.ReadKey();
    //    }

    //    static void makeTable(string cnt, string url)
    //    //static void makeTable(string cnt)
    //    {
    //        // string connectionString = cnt;
    //        string command = readCommand(url);

    //        //string command = "CREATE TABLE dbo.people (" +
    //        //               "Person int," +
    //        //               "LastName varchar(255));";

    //        try
    //        {
    //            SqlConnection conn = new SqlConnection(cnt);
    //            SqlCommand cmd = new SqlCommand(command, conn);
    //            conn.Open();
    //            cmd.ExecuteNonQuery();
    //            Console.WriteLine("New table created.");
    //            conn.Close();

    //        }
    //        catch (Exception Ex)
    //        {
    //            Console.WriteLine("Error: " + Ex.Message);

    //        }
    //    }
    
    //    static string readCommand(string url)
    //    {
    //        string text = "";
    //        using (StreamReader sr = new StreamReader(url))
    //        {
    //            text = sr.ReadToEnd();
    //        }
    //        // Console.WriteLine(text);
    //        return text;
    //        //try
    //        //{
    //        //    using (StreamReader sr = new StreamReader(url))
    //        //        {
    //        //        String location = sr.ReadtoEnd();
    //        //        return location;
    //        //    }
    //        //}
    //        //catch (Exception ex)
    //        //{
    //        //    Console.WriteLine("Command file cannot be read.");
    //        //    Console.WriteLine(ex.Message);
    //        //}
            
    //    }
        
    //    // set up bulk copy
    //    static void loader(string conn)
    //    {
    //        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn)) {
    //            bulkCopy.DestinationTableName = "dbo.";
    //        }
    //    }
//    //}   
//}
