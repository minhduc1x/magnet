using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.OleDb;
using Npgsql;
using Microsoft.VisualBasic.FileIO;

namespace DacsLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            DataSet ds = new DataSet();

            // Set up connections
            var connString = "Host=Evprodtrep01;Username=dacs_system;Password=dacsdeer;Database=mdsl";
            string dir = @"C:\Users\tranda\Downloads\Files\";
            string csvConnectionString = string.Format("Provider = Microsoft.Jet.OLEDB.4.0; Data Source = {0};Extended Properties = \"text;HDR=Yes;FMT=Delimited\";", dir);

            bool isEmpty = true;
            string filename = "MDM_service_catalog";
            //string filename = "RPM_Catalog";
            //string filename = "RPM_Catalog_20170912"; 
            string csvFile = dir + filename + ".csv";
            

            /*
             * 1. Update new items to details and vendors tables.
             * 2. Load .csv to a datatable     
             * 3. Update ID in the datatable
             * 4. Compare the datatable with database. Load the updated items. 
             */

            // 1. update new items to mdsl.details, mdsl.vendors
            updateItems(csvFile, connString, "detail2", "details", "details_temp", 7); 
            updateItems(csvFile, connString, "supplier", "vendors", "vendors_temp", 6); 

            // 2. load new .csv file to temp ds.data table
            csvToDataSet("data", csvConnectionString, csvFile, ds);
            
            // 2.5 Copy mdsl.details and mdsl.vendors to ds dataset
            sqlToDataSet("detailTable", connString, "details", ds);
            sqlToDataSet("vendorTable", connString, "vendors", ds);

            // 3. Update detailid, vendorid in ds.data
            foreach (DataRow dr in ds.Tables["data"].Rows)
            {
                string valueDetail = dr["detail2"].ToString();
                string valueVendor = dr["supplier"].ToString();

                DataRow foundRow1 = ds.Tables["detailTable"].Rows.Find(valueDetail);
                dr["detailid"] = foundRow1[0];

                DataRow foundRow2 = ds.Tables["vendorTable"].Rows.Find(valueVendor);
                dr["vendorid"] = foundRow2[0];
            }

            //Console.WriteLine(ds.Tables["data"].Rows[0]["detailid"]);
            //Console.WriteLine(ds.Tables["data"].Rows[0]["description"]);
            Console.WriteLine("Vendors and Details' IDs in ds.data are updated!");
            
            /*
             * 4. If mdsl.servicecatalog is empty: load directly from ds.temp to mdsl.servicecatalog
             * else: load ds.data to mdsl.servicecatalog_temp, save deltas to .csv file, compare, then load to ds.servicecatalog             *      
             */
            if (isEmpty)
            {
                sqlLoader("data", "servicecatalog", ds, connString);
                Console.WriteLine("New data is updated to mdsl.servicecatalog.");
            } else
            {
                sqlLoader("data", "servicecatalog_temp", ds, connString);
                delta("servicecatalog", "servicecatalog_temp", connString, filename, dir);
                sqlUpdate("servicecatalog", "servicecatalog_temp", connString);
                truncateTable("servicecatalog_temp",connString);
                Console.WriteLine("Update items are added.");
            }

            Console.ReadLine();
        }

        private static void truncateTable(string table, string connectionString)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                var cmd = new NpgsqlCommand();
                string insertTxt = "truncate servicecatalog_temp";
                cmd.CommandText = insertTxt;
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }

        private static void sqlUpdate(string dataTable, string tempTable, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                var cmd = new NpgsqlCommand();
                string insertTxt = String.Format(@"update {0} 
                                        set unitcost = new.new_unitcost, sitecost = new.new_sitecost, permcode = new.new_permcode
                                        from (SELECT A.description, A.detailid, A.vendorid, 
                                              A.unitcost as new_unitcost, B.unitcost, A.sitecost as new_sitecost, 
                                              B.sitecost, A.permcode as new_permcode, B.permcode, A.catitemid, A.objectitemno
                                              FROM {1} A
                                              INNER JOIN {0} B  ON A.catitemid = B.catitemid
                                                WHERE A.unitcost != B.unitcost
                                                OR A.sitecost != B.sitecost
                                                OR A.permcode != B.permcode) as new
                                        where {0}.catitemid = new.catitemid", dataTable, tempTable);
                cmd.CommandText = insertTxt;
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }

        private static void delta(string dataTable, string tempTable, string connectionString, string filename, string dir)
        {
            DataTable tbl = new DataTable();

            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                var cmd = new NpgsqlCommand();

                string cmdText = String.Format(@"SELECT A.description, A.detailid, A.vendorid, 
                                            A.unitcost as new_unitcost, B.unitcost, A.sitecost as new_sitecost, 
                                            B.sitecost, A.permcode as new_permcode, B.permcode, A.catitemid, A.objectitemno
                                        FROM {0} A
                                        INNER JOIN {1} B  ON A.catitemid = B.catitemid
                                            WHERE A.unitcost != B.unitcost
                                            OR A.sitecost != B.sitecost
                                            OR A.permcode != B.permcode ", tempTable, dataTable);

                cmd.CommandText = cmdText;
                cmd.Connection = conn;
                NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(cmd);

                adapter.Fill(tbl);
            }

            StringBuilder sb = new StringBuilder();

            IEnumerable<string> columnNames = tbl.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName);
            sb.AppendLine(string.Join(",", columnNames));

            foreach (DataRow row in tbl.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field =>
                string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""));
                sb.AppendLine(string.Join(",", fields));
            }

            File.WriteAllText(dir + filename + "_delta.csv", sb.ToString());
        }

        private static void sqlLoader(string tableName, string sqlTable, DataSet ds, string connectionString)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                var cmd = new NpgsqlCommand();
                string columnlist = "detailid, vendorid, description, unitcost, sitecost, permcode, catitemid, objectitemno";
                string values = "@a, @b, @c, @d, @e, @f, @g, @h";
                string insertCmd = string.Format(@"insert into {0} ({1}) values ({2});", sqlTable, columnlist, values);
                cmd.CommandText = insertCmd;
                cmd.Connection = conn;

                foreach (DataRow row in ds.Tables[tableName].Rows)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@a", row["detailid"]);
                    cmd.Parameters.AddWithValue("@b", row["vendorid"]);
                    cmd.Parameters.AddWithValue("@c", row["description"]);
                    cmd.Parameters.AddWithValue("@d", row["unitcost"]);
                    cmd.Parameters.AddWithValue("@e", row["sitecost"]);
                    cmd.Parameters.AddWithValue("@f", row["permcode"]);
                    cmd.Parameters.AddWithValue("@g", row["catitemid"]);
                    cmd.Parameters.AddWithValue("@h", row["objectitemno"]);
                    int inserted = cmd.ExecuteNonQuery();
                }
            } 
        }

        private static void sqlToDataSet(string tableName, string connString, string sqlTable, DataSet ds)
        {
            using(var conn = new NpgsqlConnection(connString)) {
                conn.Open();

                DataTable detailTbl = new DataTable();
                detailTbl.TableName = tableName;

                string cmdText = String.Format("SELECT * FROM {0}", sqlTable);
                NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);

                NpgsqlDataReader reader = cmd.ExecuteReader();
                detailTbl.Load(reader);
                ds.Tables.Add(detailTbl);

                Console.WriteLine(String.Format("{0} is loaded to {1}.{2}.",sqlTable, ds, tableName));

                ds.Tables[tableName].Constraints.Add("pk", ds.Tables[tableName].Columns["name"], true);

                conn.Close();
            }
        }

        private static void updateItems(string fileName, string ConnectionString, string columnName, string dataTable, string tempTable, int fieldNo)
         {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();

                DataTable thisCol = new DataTable();

                TextFieldParser reader = new TextFieldParser(fileName);
                reader.HasFieldsEnclosedInQuotes = true;
                reader.SetDelimiters(",");
               

                string[] fields;
                HashSet<String> nameSet = new HashSet<string>();

                while (!reader.EndOfData)
                {
                    fields = reader.ReadFields();
                    nameSet.Add(fields[fieldNo]);
                }
                nameSet.Remove(columnName);

                // load columnName to tempTable
                using (var writer = conn.BeginBinaryImport(String.Format("COPY {0} (newname) FROM STDIN (FORMAT BINARY)", tempTable)))
                {
                    foreach (var each in nameSet)
                    {
                        writer.StartRow();
                        writer.Write(each.ToString());
                        // Console.WriteLine(each.ToString());
                    }
                }

                Console.WriteLine("Loaded to temp table!");

                String insertCmd = String.Format(@"insert into {0} (name)  
                                                        select distinct newname
                                                        from {1} tmp
                                                        where 
                                                            not exists (select * from {0} where tmp.newname = {0}.name)
                                                        ", dataTable, tempTable );

                NpgsqlCommand cmd = new NpgsqlCommand(insertCmd,conn);
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;
                cmd.ExecuteReader();

                Console.WriteLine(String.Format("Column {0} of {1} is updated to {2}.", columnName, fileName, dataTable));

                reader.Close();
                conn.Close();
            }
        }

        private static void csvToDataSet(string tableName, string ConnectionString, string csvFile, DataSet ds)
        {
            using (OleDbConnection csvConn = new OleDbConnection(ConnectionString))
            {
                csvConn.Open();

                string selectCmd = "SELECT * FROM [" + Path.GetFileName(csvFile) + "]";
                DataTable tbl = new DataTable();
                tbl.TableName = tableName;

                using (var cmd = new OleDbCommand(selectCmd, csvConn))
                {
                    OleDbDataAdapter adapter = new OleDbDataAdapter(cmd);
                    adapter.Fill(tbl); // fill table
                }

                // add vendors and details cols to the table
                DataColumn vendorID = new DataColumn("vendorid", typeof(int));
                DataColumn detailID = new DataColumn("detailid", typeof(int));
                tbl.Columns.Add(detailID);
                tbl.Columns.Add(vendorID);

                ds.Tables.Add(tbl);

                ds.Tables[tableName].Constraints.Add("pk", ds.Tables[tableName].Columns["catitem"], true);
                
                //Console.WriteLine(ds.Tables[tableName].Rows[0]["ordercode"]);
                Console.WriteLine(String.Format("{0} is copied to ds.{1}.", Path.GetFileNameWithoutExtension(csvFile), tableName));

                ds.Tables[tableName].Columns["orderdesc"].ColumnName = "description";
                ds.Tables[tableName].Columns["schpermcode"].ColumnName = "permcode";
                ds.Tables[tableName].Columns["catitem"].ColumnName = "catitemid";

                csvConn.Close();
            }

            
        }
    }
}
