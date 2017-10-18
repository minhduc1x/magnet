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
             * Set up connection to Sql database and Excel file.
             * 
             * user: tranda
             * pw: Ekb4Fpr3s27w79GN
             * file: backtest.xlsx
             * sheet: LSEQ
             * backtestId: 2
             * QuantSubtrategyID: 9 and 13
             * reportedDate: 08/31/2017
             * actualFlag: 0
             * data column: A3:O
             */

            string username = "tranda";
            string password = "Ekb4Fpr3s27w79GN";
            string ExcelFile = @"C:\Users\tranda\Downloads\load\PRA and LSEQ Back Test Daily Return_20171004.xlsx";
            string sheetname = "LSEQ";
            string dataCol = "A2:O";

            // identifers for the 1st dataset 
            int backtestId = 2;
            string reportedDate = "08/31/2017";
            int quantSub = 9;
            int actualFlag = 0;

            /*
            // identifers for the 2nd dataset
            int backtestId = 2;
            string reportedDate = "08/31/2017";
            int quantSub = 13; 
            int actualFlag = 0;
            */

            //string sqlConn = @"Data Source=(localdb)\mylocaldb;Integrated Security=True";
            string sqlConn = String.Format(@"Data Source=devEXT\ext;Initial Catalog=CCDB;Persist Security Info=True;User ID={0};Password={1}", username, password);
            string ExcelConn = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Extended Properties=\"Excel 12.0 Xml;HDR=Yes;IMEX=1\";", ExcelFile);

            using (OleDbConnection excelConn = new OleDbConnection(ExcelConn))
            {
                // Read from excel sheet
                excelConn.Open();
                string cmdText = String.Format("Select * FROM [{0}${1}]", sheetname, dataCol);
                OleDbCommand cmd = new OleDbCommand(cmdText, excelConn);
                OleDbDataReader reader = cmd.ExecuteReader();

                //Copy from reader to datatable and update identifiers.
                DataTable table = new DataTable();
                table.Load(reader);

                System.Data.DataColumn BacktestIDCol = new System.Data.DataColumn("BacktestID", typeof(System.Int32));
                BacktestIDCol.DefaultValue = backtestId;
                table.Columns.Add(BacktestIDCol);

                System.Data.DataColumn ActualFlagCol = new System.Data.DataColumn("ActualFlag", typeof(System.Int32));
                ActualFlagCol.DefaultValue = actualFlag;
                table.Columns.Add(ActualFlagCol);

                System.Data.DataColumn updateIDCol = new System.Data.DataColumn("updateID", typeof(System.String));
                updateIDCol.DefaultValue = username;
                table.Columns.Add(updateIDCol);

                System.Data.DataColumn UpdateDateTimeCol = new System.Data.DataColumn("UpdateDateTime", typeof(System.DateTime));
                UpdateDateTimeCol.DefaultValue = DateTime.Now;
                table.Columns.Add(UpdateDateTimeCol);

                System.Data.DataColumn BacktestReportDateCol = new System.Data.DataColumn("BacktestReportDate", typeof(System.String));
                BacktestReportDateCol.DefaultValue = String.Format("{0} 00:00:00.0000000", reportedDate);
                table.Columns.Add(BacktestReportDateCol);

                System.Data.DataColumn quantSubCol = new System.Data.DataColumn("QuantSubStrategyID", typeof(System.String));
                quantSubCol.DefaultValue = quantSub;
                table.Columns.Add(quantSubCol);

                // Console.WriteLine(table.Rows[0][15]); 

                // Edit mapping
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(sqlConn))
                {
                    bulkcopy.DestinationTableName = "dbo.QuantBacktestDetail";

                    bulkcopy.ColumnMappings.Add(0, "DataDate");
                    
                    // mapping for 1st dataset in LSEQ
                    bulkcopy.ColumnMappings.Add(1, "LMV");
                    bulkcopy.ColumnMappings.Add(2, "SMV");
                    bulkcopy.ColumnMappings.Add(3, "ReturnGross");
                    bulkcopy.ColumnMappings.Add(4, "ReturnBetaHedged");
                    bulkcopy.ColumnMappings.Add(5, "ReturnFactorHedged");
                    bulkcopy.ColumnMappings.Add(6, "ReturnBarraIdio");
                    bulkcopy.ColumnMappings.Add(7, "NumPos");

                    /*
                    // mapping for 2nd dataset in LSEQ
                    bulkcopy.ColumnMappings.Add(8, "LMV");
                    bulkcopy.ColumnMappings.Add(9, "SMV");
                    bulkcopy.ColumnMappings.Add(10, "ReturnGross");
                    bulkcopy.ColumnMappings.Add(11, "ReturnBetaHedged");
                    bulkcopy.ColumnMappings.Add(12, "ReturnFactorHedged");
                    bulkcopy.ColumnMappings.Add(13, "ReturnBarraIdio");
                    bulkcopy.ColumnMappings.Add(14, "NumPos");
                    */                

                    bulkcopy.ColumnMappings.Add(15, "BacktestID");
                    bulkcopy.ColumnMappings.Add(16, "ActualFlag");
                    bulkcopy.ColumnMappings.Add(17, "UpdateID");
                    bulkcopy.ColumnMappings.Add(18, "UpdateDatetime");
                    bulkcopy.ColumnMappings.Add(19, "BacktestReportDate");
                    bulkcopy.ColumnMappings.Add(20, "QuantSubStrategyID");

                    bulkcopy.WriteToServer(table);
                }
                excelConn.Close();

            }
            Console.WriteLine("Table is copied.");
            Console.ReadLine();
        }
    }
}