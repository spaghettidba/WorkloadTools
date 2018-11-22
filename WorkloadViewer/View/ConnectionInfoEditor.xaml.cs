using System;
using System.Collections.Generic;
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

namespace WorkloadViewer.View
{
    /// <summary>
    /// Interaction logic for ConnectionInfoEditor.xaml
    /// </summary>
    public partial class ConnectionInfoEditor : UserControl
    {
        public ConnectionInfoEditor()
        {
            InitializeComponent();
        }

        private void Baseline_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (this.DataContext != null)
            { ((dynamic)this.DataContext).BaselinePassword = ((PasswordBox)sender).Password; }
        }

        private void Benchmark_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (this.DataContext != null)
            { ((dynamic)this.DataContext).BenchmarkPassword = ((PasswordBox)sender).Password; }
        }
    }
}
