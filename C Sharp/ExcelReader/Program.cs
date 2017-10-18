using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Logging.Configuration;
using OfficeOpenXml;
using System.IO;

namespace ExcelReader
{
    class Program : IDisposable
    {    
        static void Main(string[] args)
        {

            log4net.Config.XmlConfigurator.Configure();
            var properties = new Common.Logging.Configuration.NameValueCollection
            {
                { "configType", "INLINE" }
            };

            Common.Logging.LogManager.Adapter = new Common.Logging.Log4Net.Log4NetLoggerFactoryAdapter(properties);

            try
            {
                Options options = new Options();
                if (args.Count() == 0 || !Parser.Default.ParseArguments(args, options))
                {
                    throw new Exception("Failed to parse input params");
                }
                using (var p = new Program(options))
                {
                    p.Run(options.FilePath);
                }
            }
            catch (Exception ex)
            {

                var error = string.Format("Updater finished with errors : {0}", ex.ToString());
                Common.Logging.LogManager.GetLogger("Magnetar.Logger").ErrorFormat(error);
                return;
            }
        }

        public Program(Options options)
        {

        }

        public void Run(string filePath)
        {

            Common.Logging.ILog log = Common.Logging.LogManager.GetLogger("Magnetar.Logger");

          

            var package = new ExcelPackage();

            using (var stream = File.OpenRead(filePath))
            {
                package.Load(stream);
            }

            ExcelWorksheet workSheet = package.Workbook.Worksheets[1];

            Dictionary<int, string> columns = new Dictionary<int, string>();


            for (int i = workSheet.Dimension.Start.Row;
                     i <= workSheet.Dimension.End.Row;
                     i++)
            {
                var row = workSheet.Cells[string.Format("{0}:{0}", i)];
                bool allEmpty = row.All(c => string.IsNullOrWhiteSpace(c.Text));
                if (allEmpty) continue; // skip this row


                string[] vals = new string[workSheet.Dimension.End.Column];
                for (int j = workSheet.Dimension.Start.Column; j <= workSheet.Dimension.End.Column; j++)
                {
                    if (i == 1)
                        columns.Add(j, workSheet.Cells[i, j].Text);

                    object cellValue = workSheet.Cells[i, j].Value;

                    var val = cellValue ?? ProcessObject(cellValue);
                    vals[j - 1] = val.ToString();

                }
                log.InfoFormat(string.Join("|", vals));
            }
        }

        private object ProcessObject( object cellValue)
        {
            if (cellValue == null) return "";
            double dbl = 0;
            if (double.TryParse(cellValue.ToString(), out dbl))
                return  (double?)dbl;

            DateTime dt = new DateTime();
            if (DateTime.TryParse(cellValue.ToString(), out dt))
                return (DateTime?)dt;

            decimal dec = new decimal();
            if (decimal.TryParse(cellValue.ToString(), out dec))
                return (decimal?)dec;

            float flt = new float();
            if (float.TryParse(cellValue.ToString(), out flt))
                return (float?)flt;

            return cellValue.ToString();

       }
        public void Dispose()
        {

        }
    }
}
