using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using GalaSoft.MvvmLight.Messaging;
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
        private string _baselineServer;
        private string _baselineDatabase;

        public string BaselineServer {
            get { return _baselineServer; }
            set {
                _baselineServer = value;
                if (String.IsNullOrEmpty(BenchmarkServer))
                {
                    BenchmarkServer = _baselineServer;
                    RaisePropertyChanged("BenchmarkServer");
                }
                    
            }
        }
        public string BaselineDatabase
        {
            get { return _baselineDatabase; }
            set
            {
                _baselineDatabase = value;
                if (String.IsNullOrEmpty(BenchmarkDatabase))
                {
                    BenchmarkDatabase = _baselineDatabase;
                    RaisePropertyChanged("BenchmarkDatabase");
                }

            }
        }
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
        public ICommand KeyDownCommand { get; set; }

        public bool Cancel = false;
        private IDialogCoordinator _dialogCoordinator;
        public Exception Exception;
        public MainViewModel Context;
        public BaseMetroDialog Dialog;

        public ConnectionInfoEditorViewModel()
        {
            CancelCommand = new RelayCommand<RoutedEventArgs>(Cancel_Pressed);
            OKCommand = new RelayCommand<RoutedEventArgs>(OK_Pressed);
            KeyDownCommand = new RelayCommand<KeyEventArgs>(KeyDown);
            _dialogCoordinator = DialogCoordinator.Instance;
            Cancel = false;
            Exception = null;
        }

        private void KeyDown(KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                var msg = new Message("OK");
                Messenger.Default.Send<Message>(msg);
            }
        }

        private async void Cancel_Pressed(RoutedEventArgs e)
        {
            Cancel = true;
            await _dialogCoordinator.HideMetroDialogAsync(Context, Dialog);
            //App.Current.Shutdown();
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
                if(Dialog.IsVisible)
                    await _dialogCoordinator.HideMetroDialogAsync(Context, Dialog);
            }
        }
    }
}
