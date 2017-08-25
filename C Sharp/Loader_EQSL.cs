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
            // string sqlConnectionString = @"Data Source=devEXT\ext;Initial Catalog=CCDB;Persist Security Info=True;User ID=tranda;Password=Ekb4Fpr3s27w79GN";
            string sqlConnectionString = @"Data Source=(localdb)\mylocaldb;Integrated Security=True";
            string ExcelFileName = @"C:\Users\tranda\Desktop\Files\EQLS_Monthly 6 Strategies Combination_20170613.xlsx";
            string ExcelConnectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Extended Properties=\"Excel 12.0 Xml;HDR=Yes;IMEX=1\";", ExcelFileName);

            using (OleDbConnection excelConn = new OleDbConnection(ExcelConnectionString))
            {
                // Read from Ret Daily Stat sheet
                excelConn.Open();
                OleDbCommand sheet2 = new OleDbCommand
                                    ("Select * FROM [Ret Daily Stat$A2:BL]", excelConn);
                OleDbDataReader reader2 = sheet2.ExecuteReader();

                /*
                 * Set backtestID, QuantSubStrategyID, ActualFlag, updateID, and UpdateDateTime 
                 * for the datatable.
                 *
                 * File: EQLS_Monthly 6 Strategies Combination_20170613
                 * QuantStrategyID: 2
                 * BacktestID: 2
                 * BacktestDesc: EQLS
                 * QuantSubStrategyID = 2   : Stock Buyback > 10%, Stock Level
                 * QuantSubStrategyID = 3   : Debt Reduction > 2%, Stock Level
                 * QuantSubStrategyID = 4   : Debt Issue > 20%, Stock Level
                 * QuantSubStrategyID = 5   : GPOA Bot 10%, Stock Level
                 * QuantSubStrategyID = 6   : SIOverFloat > 20%, Stock Level
                 * QuantSubStrategyID = 7   : Idio Vol Top 10%, Stock Level
                 * QuantSubStrategyID = 8   : EQLS, Stock Level, 0.2 Beta
                 * QuantSubStrategyID = 9   : EQLS, Stock Level, 0 Beta
                 * QuantSubStrategyID = 10  : EQLS, Stock Level, 0 Beta
                 */

                DataTable table = new DataTable("temp");
                table.Load(reader2);
                //table.Columns.Add("BacktestID", typeof(int));
                //table.Columns.Add("QuantSubStrategyID", typeof(int));
                //table.Columns.Add("ActualFlag",typeof(int));
                //table.Columns.Add("updateID",typeof(string));
                //table.Columns.Add("UpdateDateTime",typeof(DateTime)); // <<< OMG

                System.Data.DataColumn BacktestIDCol = new System.Data.DataColumn("BacktestID", typeof(System.String));
                BacktestIDCol.DefaultValue = 2;
                table.Columns.Add(BacktestIDCol);

                System.Data.DataColumn QuantSubStrategyIDCol = new System.Data.DataColumn("QuantSubStrategyID", typeof(System.String));
                QuantSubStrategyIDCol.DefaultValue = 2;
                table.Columns.Add(QuantSubStrategyIDCol);

                System.Data.DataColumn ActualFlagCol = new System.Data.DataColumn("ActualFlag", typeof(System.String));
                ActualFlagCol.DefaultValue = 2;
                table.Columns.Add(ActualFlagCol);

                System.Data.DataColumn updateIDCol = new System.Data.DataColumn("updateID", typeof(System.String));
                updateIDCol.DefaultValue = "daniel";
                table.Columns.Add(updateIDCol);

                //System.Data.DataColumn UpdateDateTimeCol = new System.Data.DataColumn("UpdateDateTime", typeof(System.DateTime));
                //UpdateDateTimeCol.DefaultValue = DateTime.Now;
                //table.Columns.Add(UpdateDateTimeCol);


                // Bulk copy to datebase
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(sqlConnectionString))
                {
                    bulkcopy.DestinationTableName = "dbo.QuantBacktestDetail";

                    bulkcopy.ColumnMappings.Add(0, "BacktestReportDate");
                    bulkcopy.ColumnMappings.Add(1,"LMV");
                    bulkcopy.ColumnMappings.Add(2, "SMV");
                    // bulkcopy.ColumnMappings.Add(3, );
                    bulkcopy.ColumnMappings.Add(4,);

                    bulkcopy.ColumnMappings.Add(64, "BacktestID");
                    bulkcopy.ColumnMappings.Add(65, "QuantSubStrategyID");
                    bulkcopy.ColumnMappings.Add(66, "ActualFlag");
                    bulkcopy.ColumnMappings.Add(67, "updateID");
                    // bulkcopy.ColumnMappings.Add(68, "UpdateDateTime");

                    bulkcopy.WriteToServer(table);
                }
                excelConn.Close();        
            }

            Console.WriteLine("Table is copied.");
            Console.ReadLine();
        }
    }
}