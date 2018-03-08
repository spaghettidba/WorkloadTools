using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener
{
    public class SqlTransformer
    {
        public string Transform(string command)
        {
            // remove the handle from the sp_prepexec call
            if (command.Contains("sp_prepexec "))
            {
                command = RemoveFirstP1(command);
                command += " ; EXEC sp_unprepare @p1;";
            }


            //  remove the handle from the sp_cursoropen call
            if (command.Contains("sp_cursoropen "))
            {
                command = RemoveFirstP1(command);
                command += " ; EXEC sp_cursorclose @p1;";
            }
            return command;
        }



        public bool Skip(string command)
        {
            // skip reset connection commands
            if (command.Contains("sp_reset_connection"))
                return true;

            // skip unprepare commands
            if (command.Contains("sp_unprepare "))
                return true;

            // skip cursor fetch
            if (command.Contains("sp_cursorfetch "))
                return true;

            // skip cursor close
            if (command.Contains("sp_cursorclose "))
                return true;

            // skip sp_execute
            if (command.Contains("sp_execute "))
                return true;

            return false;
        }


        private string RemoveFirstP1(string command)
        {
            int idx = command.IndexOf("set @p1=");
            if (idx > 0)
            {
                StringBuilder sb = new StringBuilder(command);

                // replace "set @p1=" with whitespace
                for (int i = 0; i < 8; i++)
                {
                    sb[idx++] = ' ';
                }

                while (Char.IsNumber(sb[idx]))
                {
                    sb[idx] = ' ';
                    idx++;
                }
                command = sb.ToString();
            }
            return command;
        }
    }
}
