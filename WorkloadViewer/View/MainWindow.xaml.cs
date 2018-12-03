using MahApps.Metro.Controls;
using System;
using System.Collections.Generic;
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

        private void DataGridDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if(((DataGrid)sender).SelectedItem == null)
            {
                return;
            }
            Dispatcher.BeginInvoke((Action)(() => MainTabControl.SelectedIndex = 2));
        }
    }
}
