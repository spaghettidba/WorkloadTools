using GalaSoft.MvvmLight.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using WorkloadViewer.ViewModel;

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

            Messenger.Default.Register<Message>(this, (msg) => ReceiveMessage(msg));
        }

        private void ReceiveMessage(Message msg)
        {
            if(msg.Text == "OK")
            {
                //Fist of all, remove focus from the current text control and set it to the button
                Keyboard.Focus(OKButton);
                // Then fire the click event and its associated command
                ButtonAutomationPeer peer = new ButtonAutomationPeer(OKButton);
                IInvokeProvider invokeProv = peer.GetPattern(PatternInterface.Invoke) as IInvokeProvider;
                invokeProv.Invoke();
            }
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
