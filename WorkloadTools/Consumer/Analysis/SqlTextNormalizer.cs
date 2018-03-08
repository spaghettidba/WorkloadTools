using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace WorkloadTools.Consumer.Analysis
{
    public class SqlTextNormalizer
    {

        private static Hashtable prepSql = new Hashtable();

        private static Regex _doubleApostrophe = new Regex("('')(?<string>.*?)('')", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace | RegexOptions.CultureInvariant);
        private static Regex _delimiterStart = new Regex("(--)|(/\\*)|'", RegexOptions.Compiled);
        private static Regex _spreadCsv = new Regex(",(?=\\S)", RegexOptions.Compiled);
        private static Regex _spaces = new Regex("\\s+", RegexOptions.Compiled);
        private static Regex _blockComment = new Regex("/\\*.*?\\*/", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace | RegexOptions.CultureInvariant);
        private static Regex _blockCommentDelimiters = new Regex("/\\*|\\*/", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace | RegexOptions.CultureInvariant);
        private static Regex _inlineComment = new Regex("--.*$", RegexOptions.Multiline | RegexOptions.Compiled);
        private static Regex _prepareSql = new Regex("EXEC\\s+(?<preptype>SP_PREP(ARE|EXEC))\\s+@P1\\s+OUTPUT,\\s*(NULL|(N\\'.+?\\')),\\s*N(?<remaining>.+)$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _prepExecRpc = new Regex("SET\\s+@P1=(?<stmtnum>\\d+)\\s+EXEC\\s+SP_PREPEXECRPC\\s+@P1\\s+OUTPUT,\\s*N\\'(?<statement>.+?)'", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _preppedSqlStatement = new Regex("^(')(?<statement>((?!\\1).|\\1{2})*)\\1", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _execPrepped = new Regex("^EXEC\\s+SP_EXECUTE\\s+(?<stmtnum>\\d+)", RegexOptions.Compiled);
        private static Regex _execUnprep = new Regex("EXEC\\s+SP_UNPREPARE\\s+(?<stmtnum>\\d+)", RegexOptions.Compiled);
        private static Regex _cursor = new Regex("EXEC\\s+SP_CURSOROPEN\\s+(@CURSOR\\s*=\\s*)?\\@P1\\s+OUTPUT\\,\\s*(@STMT\\s*=\\s*)?(N)?(?<tick>')  (?<statement>      ((  (?!\\k<tick>)  .|\\k<tick>{2})*)   )    \\k<tick>", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace);
        private static Regex _cursorPrepExec = new Regex("EXEC\r\n\\s+     # any spaces\r\nsp_cursorprepexec\r\n.+       # any characters up to the string\r\nN  \r\n(?<tick>')   # matches an apostraphe\r\n(?!@)    # but no @ following\r\n(?<statement>   ((  (?!\\k<tick>)  .|\\k<tick>{2})*)   )    # all the characters ...\r\n\\k<tick>   # until the next tick that isn't doubled.", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace);
        private static Regex _spExecuteSql = new Regex("EXEC\\s+SP_EXECUTESQL\\s+N\\'(?<statement>.+?)\\'", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _spExecuteSqlWithStatement = new Regex("EXEC\\s+SP_EXECUTESQL\\s+@statement\\s*=\\s*N\\'(?<statement>.+?)\\'", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _objectName = new Regex("EXEC(UTE){0,1}\\s(?<schema>(\\w+\\.)*)(?<object>\\w+)", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _dbAndObjectName = new Regex("EXEC(UTE){0,1}\\s+(?<database>\\w+)\\.\\.(?<object>\\w+)", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _emptyString = new Regex("\\'\\'", RegexOptions.Compiled);
        private static Regex _unicodeConstant = new Regex("N{STR}", RegexOptions.Compiled);
        private static Regex _stringConstant = new Regex("(')(((?!\\1).|\\1{2})*)\\1", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _binaryConstant = new Regex("0X([0-9ABCDEF])+", RegexOptions.Compiled);
        private static Regex _numericConstant = new Regex("(?<prefix>[\\(\\s,=\\-><\\!\\&\\|\\+\\*\\/\\%\\~\\$])(?<digits>[\\-\\.\\d]+)", RegexOptions.Compiled);
        private static Regex _inClause = new Regex("IN\\s*\\(\\s*\\{.*\\}\\s*\\)", RegexOptions.Compiled | RegexOptions.Singleline);
        private static Regex _brackets = new Regex("(\\[|\\])", RegexOptions.Compiled);

        private static bool TRUNCATE_TO_4000 = false;
        private static bool TRUNCATE_TO_1024K = false;

        public NormalizedSqlText NormalizeSqlText(string sql, string eventClass, int spid)
        {
            return NormalizeSqlText(sql, eventClass, spid, true);
        }


        public NormalizedSqlText NormalizeSqlText(string sql, string eventClass, int spid, bool spreadCsv)
        {
            NormalizedSqlText result = new NormalizedSqlText();
            result.OriginalText = sql;
            result.NormalizedText = sql;

            if (sql == null)
            {
                result.OriginalText = "";
                result.NormalizedText = "";
                result.Statement = "";
                return result;
            }
            if (TRUNCATE_TO_1024K  && sql.Length > 1024000)
            {
                result.Statement = "{SQL>1MB}";
                result.NormalizedText = "{SQL>1MB}";
                return result;
            }
                
            bool flag1 = false;
            bool flag2 = false;
            int num = 0;

            if (sql == "sp_reset_connection")
                return null;

            sql = FixComments(sql);
            sql = _spaces.Replace(sql, " ").ToUpper(CultureInfo.InvariantCulture);
            Match match1 = null;
            if (eventClass == "RPCCompleted" || eventClass == "RPCStarted")
            {
                sql = _doubleApostrophe.Replace(sql, "{STR}");
                Match match2 = _prepExecRpc.Match(sql);
                if (match2.Success)
                {
                    sql = match2.Groups["statement"].ToString();
                    result.Statement = sql;
                    result.NormalizedText = sql;
                }
                Match match3 = _prepareSql.Match(sql);
                if (match3.Success)
                {
                    if (match3.Groups["preptype"].ToString().ToLower() == "sp_prepare")
                        flag2 = true;
                    //num = !(match3.Groups["stmtnum"].ToString() == "NULL") ? Convert.ToInt32(match3.Groups["stmtnum"].ToString()) : 0;
                    sql = match3.Groups["remaining"].ToString();
                    Match match4 = _preppedSqlStatement.Match(sql);
                    if (match4.Success)
                    {
                        sql = match4.Groups["statement"].ToString();
                        sql = _doubleApostrophe.Replace(sql, "'${string}'");
                        result.Statement = sql;
                        result.NormalizedText = sql;
                    }
                    flag1 = true;
                }
                match1 = (Match)null;
                Match match5 = _execPrepped.Match(sql);
                if (match5.Success)
                {
                    num = Convert.ToInt32(match5.Groups["stmtnum"].ToString());
                    if (prepSql.ContainsKey((object)(spid.ToString() + "_" + num.ToString())))
                    {
                        result.NormalizedText = TruncateSql("{PREPARED} " + prepSql[(object)(spid.ToString() + "_" + num.ToString())].ToString());
                        return result;
                    }
                }
                match1 = (Match)null;
                Match match6 = _execUnprep.Match(sql);
                if (match6.Success)
                {
                    num = Convert.ToInt32(match6.Groups["stmtnum"].ToString());
                    string str = spid.ToString() + "_" + num.ToString();
                    if (prepSql.ContainsKey((object)str))
                    {
                        sql = prepSql[(object)str].ToString();
                        prepSql.Remove((object)(spid.ToString() + "_" + num.ToString()));
                        match1 = (Match)null;
                        result.NormalizedText = TruncateSql("{UNPREPARING} " + sql);
                        return result;
                    }
                }
                match1 = (Match)null;
            }
            if (eventClass == "RPCCompleted" || eventClass == "RPCStarted")
            {
                Match match2 = _cursor.Match(sql);
                if (match2.Success)
                {
                    sql = match2.Groups["statement"].ToString();
                    sql = _doubleApostrophe.Replace(sql, "'${string}'");
                    result.Statement = sql;
                    result.NormalizedText =  "{CURSOR} " + sql;
                }
                Match match3 = _cursorPrepExec.Match(sql);
                if (match3.Success)
                {
                    sql = match3.Groups["statement"].ToString();
                    sql = _doubleApostrophe.Replace(sql, "'${string}'");
                    result.Statement = sql;
                    result.NormalizedText = "{CURSOR} " + sql;
                }
                Match match4 = _spExecuteSql.Match(sql);
                if (match4.Success)
                {
                    sql = match4.Groups["statement"].ToString();
                    result.Statement = sql;
                    result.NormalizedText = sql;
                }
                match1 = (Match)null;
                Match match5 = _spExecuteSqlWithStatement.Match(sql);
                if (match5.Success)
                {
                    sql = match5.Groups["statement"].ToString();
                    result.Statement = sql;
                    result.NormalizedText = sql;
                }
                match1 = (Match)null;
                if (!_brackets.Match(sql).Success)
                {
                    Match match6 = _dbAndObjectName.Match(sql);
                    if (match6.Success)
                    {
                        sql = match6.Groups["object"].ToString();
                    }
                    else
                    {
                        Match match7 = _objectName.Match(sql);
                        if (match7.Success)
                            sql = match7.Groups["object"].ToString();
                    }
                    if (sql == "SP_CURSOR" || sql == "SP_CURSORFETCH" || (sql == "SP_CURSORCLOSE" || sql == "SP_RESET_CONNECTION"))
                    {
                        match1 = (Match)null;
                        return null;
                    }
                }
                match1 = (Match)null;
            }
            result.NormalizedText = _emptyString.Replace(result.NormalizedText, "{STR}");
            result.NormalizedText = _stringConstant.Replace(result.NormalizedText, "{STR}");
            result.NormalizedText = _unicodeConstant.Replace(result.NormalizedText, "{NSTR}");
            result.NormalizedText = _binaryConstant.Replace(result.NormalizedText, "{BINARY}");
            result.NormalizedText = _numericConstant.Replace(result.NormalizedText, "${prefix}{##}");
            result.NormalizedText = _inClause.Replace(result.NormalizedText, "{IN}");
            if (spreadCsv)
                result.NormalizedText = _spreadCsv.Replace(result.NormalizedText, ", ");
            result.NormalizedText = _spaces.Replace(result.NormalizedText, " ");
            result.NormalizedText = TruncateSql(result.NormalizedText);
            if (flag1 && num != 0)
            {
                if (!prepSql.ContainsKey((object)(spid.ToString() + "_" + num.ToString())))
                    prepSql.Add((object)(spid.ToString() + "_" + num.ToString()), (object)sql);
                else
                    prepSql[(object)(spid.ToString() + "_" + num.ToString())] = (object)sql;
            }
            match1 = (Match)null;
            if (flag2)
            {
                result.NormalizedText = TruncateSql("{PREPARING} " + sql);
                return result;
            }
            if (flag1 && !flag2)
            {
                result.NormalizedText = TruncateSql("{PREPARED} " + sql);
                return result;
            }
            result.NormalizedText = TruncateSql(result.NormalizedText);
            return result;
        }


        private string TruncateSql(string sql)
        {
            sql = sql.Trim();
            if (TRUNCATE_TO_4000 && sql.Length > 4000)
                return sql.Substring(0, 4000);
            return sql;
        }


        private string FixComments(string sql)
        {
            string str = sql;
            int num = 0;
            int startat = 0;
            Match match1 = _delimiterStart.Match(sql, startat);
            while (match1.Success)
            {
                switch (match1.Value)
                {
                    case "'":
                        Match match2 = _stringConstant.Match(sql, match1.Index);
                        if (match2.Success)
                        {
                            startat = match1.Index + match2.Length;
                            break;
                        }
                        ++startat;
                        break;
                    case "--":
                        startat = match1.Index;
                        if (_inlineComment.Match(sql, startat).Success)
                        {
                            sql = _inlineComment.Replace(sql, "", 1, startat);
                            break;
                        }
                        ++startat;
                        break;
                    case "/*":
                        int index = match1.Index;
                        sql = RemoveBlockComments(sql, index);
                        startat = index + 1;
                        break;
                    default:
                        return sql;
                }
                if (startat < sql.Length)
                {
                    match1 = _delimiterStart.Match(sql, startat);
                    ++num;
                    if (num > 1000000)
                        throw new Exception("Infinite loop in FixComments (" + Assembly.GetExecutingAssembly().GetName().Version.ToString() + ")" + Environment.NewLine + Environment.NewLine + str);
                }
                else
                    break;
            }
            return sql;
        }

        private string RemoveBlockComments(string sql, int position)
        {
            StringBuilder stringBuilder = new StringBuilder(sql.Length);
            stringBuilder.Append(sql.Substring(0, position));
            int num = 0;
            int startIndex = position;
            while (startIndex < sql.Length - 1)
            {
                switch (sql.Substring(startIndex, 2))
                {
                    case "/*":
                        ++num;
                        startIndex = startIndex + 1 + 1;
                        break;
                    case "*/":
                        --num;
                        startIndex = startIndex + 1 + 1;
                        break;
                    default:
                        ++startIndex;
                        break;
                }
                if (num == 0)
                {
                    if (startIndex < sql.Length)
                        stringBuilder.Append(sql.Substring(startIndex, sql.Length - startIndex));
                    return stringBuilder.ToString();
                }
            }
            return sql;
        }

        public long GetHashCode(string text)
        {
            text = text == null ? "" : text;
            int num = text.Length / 2;
            return (long)int.MaxValue * (long)text.Substring(0, num).GetHashCode() + (long)text.Substring(num, text.Length - num).GetHashCode();
        }

    }
}
