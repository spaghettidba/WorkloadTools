using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using MahApps.Metro.Controls.Dialogs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace WorkloadViewer.ViewModel
{
    public class ConnectionInfoEditorViewModel : ViewModelBase
    {

        public string BaselineServer { get; set; }
        public string BaselineDatabase { get; set; }
        public string BaselineSchema { get; set; }
        public string BaselineUsername { get; set; }
        public string BaselinePassword { get; set; }

        public string BenchmarkServer { get; set; }
        public string BenchmarkDatabase { get; set; }
        public string BenchmarkSchema { get; set; }
        public string BenchmarkUsername { get; set; }
        public string BenchmarkPassword { get; set; }


        public ICommand CancelCommand { get; set; }
        public ICommand OKCommand { get; set; }

        public bool Cancel = false;
        private IDialogCoordinator _dialogCoordinator;
        public Exception Exception;
        public MainViewModel Context;
        public BaseMetroDialog Dialog;

        public ConnectionInfoEditorViewModel()
        {
            CancelCommand = new RelayCommand<RoutedEventArgs>(Cancel_Pressed);
            OKCommand = new RelayCommand<RoutedEventArgs>(OK_Pressed);
            _dialogCoordinator = DialogCoordinator.Instance;
            Cancel = false;
            Exception = null;
        }

        private async void Cancel_Pressed(RoutedEventArgs e)
        {
            Cancel = true;
            await _dialogCoordinator.HideMetroDialogAsync(Context, Dialog);
            App.Current.Shutdown();
        }

        private async void OK_Pressed(RoutedEventArgs e)
        {
            Cancel = false;
            try
            {
                Context.SetConnectionInfo(this);
            }
            catch (Exception ex)
            {
                Exception = ex;
            }
            finally
            {
                await _dialogCoordinator.HideMetroDialogAsync(Context, Dialog);
            }
        }
    }
}
