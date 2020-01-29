using GalaSoft.MvvmLight.Messaging;
using ICSharpCode.AvalonEdit;
using MahApps.Metro.Controls;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using WorkloadViewer.ViewModel;
using Path = System.IO.Path;

namespace WorkloadViewer
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : MetroWindow
    {
        public MainWindow()
        {
            InitializeComponent();

            Messenger.Default.Register<SortColMessage>(this, (msg) => ReceiveSortMessage(msg));

            
            using (var stream = new MemoryStream(WorkloadViewer.Properties.Resources.TSQL))
            {
                using (var reader = new System.Xml.XmlTextReader(stream))
                {
                    var highlighting = 
                        ICSharpCode.AvalonEdit.Highlighting.Xshd.HighlightingLoader.Load(reader, ICSharpCode.AvalonEdit.Highlighting.HighlightingManager.Instance);
                    QueryText.SyntaxHighlighting = highlighting;
                    QueryDetailText.SyntaxHighlighting = highlighting;
                }
            }
        }

        private void ReceiveSortMessage(SortColMessage msg)
        {
            try
            {
                DataGridColumn dgc = Queries.Columns.First(el => el.Header.ToString().Equals(msg.ColumnName));
                if(dgc != null)
                {
                    dgc.SortDirection = msg.Direction;
                    SortDescription sd = new SortDescription(dgc.SortMemberPath, msg.Direction);
                    CollectionViewSource cvs = (CollectionViewSource)this.Resources["WorkloadQueries"];
                    cvs.SortDescriptions.Clear();
                    cvs.SortDescriptions.Add(new SortDescription(dgc.SortMemberPath, msg.Direction));
                }
            }
            catch(Exception)
            {
                //swallow
            }
        }

        private void DataGridDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if(((DataGrid)sender).SelectedItem == null)
            {
                return;
            }
            Dispatcher.BeginInvoke((Action)(() => MainTabControl.SelectedIndex = 2));
        }

        private void QueryText_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            OpenFileWithDefaultApp(sender);
        }

        private void OpenFileWithDefaultApp(object sender)
        {
            // save text to a temp file and open with windows
            try
            {
                TextEditor editor = (TextEditor)sender;
                string docPath = Path.Combine(Path.GetTempPath(), editor.Tag + ".sql");

                // Write the string array to a new file named "WriteLines.txt".
                using (StreamWriter outputFile = new StreamWriter(docPath))
                {
                    outputFile.WriteLine(editor.Text);
                }
                System.Diagnostics.Process.Start(docPath);
            }
            catch (Exception)
            {
                // swallow
            }
        }
    }
}
