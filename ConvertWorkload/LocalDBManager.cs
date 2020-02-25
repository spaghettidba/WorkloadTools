using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools;

namespace ConvertWorkload
{
    internal class LocalDBManager
    {

        public bool IsElevated
        {
            get
            {
                return new WindowsPrincipal(WindowsIdentity.GetCurrent()).IsInRole(WindowsBuiltInRole.Administrator);
            }
        }

        public string DownloadLocalDB()
        {
            string localPath = Path.GetTempPath() + "SqlLocalDB.msi";
            using (var client = new WebClient())
            {
                IWebProxy wp = WebRequest.DefaultWebProxy;
                wp.Credentials = CredentialCache.DefaultCredentials;
                client.Proxy = wp;
                client.DownloadFile("https://download.microsoft.com/download/7/c/1/7c14e92e-bdcb-4f89-b7cf-93543e7112d1/SqlLocalDB.msi", localPath);
            }
            return localPath;
        }


        public void InstallLocalDB()
        {
            if (!IsElevated)
            {
                throw new InvalidOperationException("Installing LocalDB requires elevation.");
            }
            string localFileName = DownloadLocalDB();
            Process p = new Process();
            p.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            p.StartInfo.FileName = "c:\\windows\\system32\\msiexec.exe";
            p.StartInfo.Arguments = " /i "+ localFileName +" /qn IACCEPTSQLLOCALDBLICENSETERMS=YES";
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.RedirectStandardError = true;
            //Vista or higher check
            if (System.Environment.OSVersion.Version.Major >= 6)
            {
                p.StartInfo.Verb = "runas";
            }
            p.Start();
            p.WaitForExit();
        }

        public bool CanConnectToLocalDB()
        {
            try
            {
                SqlConnectionInfo info = new SqlConnectionInfo();
                info.ServerName = @"(localdb)\MSSQLLocalDB";
                info.UseIntegratedSecurity = true;
                using (SqlConnection conn = new SqlConnection(info.ConnectionString + ";Connect Timeout=30;"))
                {
                    conn.Open();
                }
                return true;
            }
            catch(Exception)
            {
                return false;
            }
        }


    }
}
