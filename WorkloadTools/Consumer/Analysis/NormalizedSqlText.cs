using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer.Analysis
{
    public class NormalizedSqlText
    {
        public enum CommandTypeEnum
        {
            SP_EXECUTE,
            SP_PREPARE,
            SP_UNPREPARE,
            SP_CURSOR,
            OTHER,
            SP_RESET_CONNECTION
        }


        public NormalizedSqlText()
        {
            CommandType = CommandTypeEnum.OTHER;
        }

        public NormalizedSqlText(string command) : this()
        {
            NormalizedText = command;
            OriginalText = command;
            Statement = command;
            Handle = 0;
        }

        public string OriginalText { get; set; }
        public string Statement { get; set; }
        public string NormalizedText { get; set; }
        public int Handle { get; set; }
        public CommandTypeEnum CommandType { get; set; }
        internal int ReferenceCount { get; set; }

    }
}
