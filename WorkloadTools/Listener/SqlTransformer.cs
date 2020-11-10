using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using WorkloadTools.Consumer.Analysis;

namespace WorkloadTools.Listener
{
    public class SqlTransformer
    {

        private static Regex _execPrepped = new Regex("^EXEC\\s+SP_EXECUTE\\s+(?<stmtnum>\\d+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static Regex _execUnprep = new Regex("EXEC\\s+SP_UNPREPARE\\s+(?<stmtnum>\\d+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static Regex _prepareSql = new Regex("EXEC\\s+(?<preptype>SP_PREP(ARE|EXEC))\\s+@P1\\s+OUTPUT,\\s*(NULL|(N\\'.+?\\')),\\s*N(?<remaining>.+)$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _preppedSqlStatement = new Regex("^(')(?<statement>((?!\\1).|\\1{2})*)\\1", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _doubleApostrophe = new Regex("('')(?<string>.*?)('')", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace | RegexOptions.CultureInvariant);

        private static MatchEvaluator decimal38Evaluator = new MatchEvaluator(MakeFloat);

        private static string MakeFloat(Match match)
        {
            return match.Value + "E0";
        }


        public string Transform(string command)
        {
            // remove the handle from the sp_prepexec call
            if (command.Contains("sp_prepexec "))
            {
                command = RemoveFirstP1(command);
                if (!command.EndsWith("EXEC sp_unprepare @p1;"))
                    command += " ; EXEC sp_unprepare @p1;";
            }


            //  remove the handle from the sp_cursoropen call
            else if (command.Contains("sp_cursoropen "))
            {
                command = RemoveFirstP1(command);
                if (!command.EndsWith("EXEC sp_cursorclose @p1;"))
                    command += " ; EXEC sp_cursorclose @p1;";
            }

            //  remove the handle from the sp_cursorprepexec call
            else if (command.Contains("sp_cursorprepexec "))
            {
                command = RemoveFirstP1(command);
                if (!command.EndsWith("EXEC sp_cursorunprepare @p1;"))
                    command += " ; EXEC sp_cursorunprepare @p1;";
            }


            // trim numbers with precision > 38
            // rpc_completed events may return float parameters
            // as long numeric strings that exceed the maximum decimal
            // precision of 38. 
            // Any decimal numeric string in T-SQL is interpreted as decimal,
            // unless it ends with "E0", which designates a float literal. 
            // Any decimal numeric string longer than 38 characters needs to
            // be appended "E0" to be treated as float.
            command = Regex.Replace(command, @"[0-9\.]{38,}", decimal38Evaluator);

            return command;
        }



        public bool Skip(string command)
        {
            // skip reset connection commands
            //if (command.Contains("sp_reset_connection"))
            //    return true;

            // skip unprepare commands
            //if (command.Contains("sp_unprepare "))
            //    return true;

            // skip cursor fetch
            if (command.Contains("sp_cursorfetch "))
                return true;

            // skip cursor close
            if (command.Contains("sp_cursorclose "))
                return true;

            // skip cursor unprepare
            if (command.Contains("sp_cursorunprepare "))
                return true;

            // skip sp_execute
            //if (command.Contains("sp_execute "))
            //    return true;

            return false;
        }


        private string RemoveFirstP1(string command)
        {
            int idx = command.IndexOf("set @p1=");
            if (idx > 0)
            {
                StringBuilder sb = new StringBuilder(command);
                idx += 8; // move past "set @p1="

                // replace numeric chars with 0s
                while (Char.IsNumber(sb[idx]))
                {
                    sb[idx] = '0';
                    idx++;
                }
                command = sb.ToString();
            }
            return command;
        }


        public NormalizedSqlText Normalize(string command)
        {
            NormalizedSqlText result = new NormalizedSqlText(command);

            int num = 0;

            if (command.Contains("sp_reset_connection"))
            {
                result.CommandType = NormalizedSqlText.CommandTypeEnum.SP_RESET_CONNECTION;
                return result;
            }
                

            Match match3 = _prepareSql.Match(command);
            if (match3.Success)
            {
                if (match3.Groups["preptype"].ToString().ToLower() == "sp_prepare")
                {
                    if(match3.Groups["stmtnum"].Success)
                        num = !(match3.Groups["stmtnum"].ToString() == "NULL") ? Convert.ToInt32(match3.Groups["stmtnum"].ToString()) : 0;
                    string sql = match3.Groups["remaining"].ToString();
                    Match match4 = _preppedSqlStatement.Match(sql);
                    if (match4.Success)
                    {
                        sql = match4.Groups["statement"].ToString();
                        sql = _doubleApostrophe.Replace(sql, "'${string}'");
                        result.Statement = sql;
                        result.NormalizedText = RemoveFirstP1(result.OriginalText);
                        result.Handle = num;
                        result.CommandType = NormalizedSqlText.CommandTypeEnum.SP_PREPARE;
                    }
                }
                return result;
            }

            Match match5 = _execPrepped.Match(command);
            if (match5.Success)
            {
                num = Convert.ToInt32(match5.Groups["stmtnum"].ToString());
                result.Handle = num;
                result.Statement = "EXEC sp_execute";
                result.NormalizedText = "EXEC sp_execute";
                result.CommandType = NormalizedSqlText.CommandTypeEnum.SP_EXECUTE;
                return result;
            }

            Match match6 = _execUnprep.Match(command);
            if (match6.Success)
            {
                num = Convert.ToInt32(match6.Groups["stmtnum"].ToString());
                result.Handle = num;
                result.Statement = "EXEC sp_unprepare";
                result.NormalizedText = "EXEC sp_unprepare";
                result.CommandType = NormalizedSqlText.CommandTypeEnum.SP_UNPREPARE;
                return result;
            }


            return result;
        }

    }
}
