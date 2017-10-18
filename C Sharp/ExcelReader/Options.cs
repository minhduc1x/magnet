using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExcelReader
{
    public class Options
    {
        [Option('s', "FilePath", HelpText = "Please provide File Path")]
        public string FilePath { get; set; }
    }
}
