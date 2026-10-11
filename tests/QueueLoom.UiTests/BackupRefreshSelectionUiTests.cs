using Avalonia.Controls;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views.Pages;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class BackupRefreshSelectionUiTests
{
    [Fact]
    public Task BackupRefresh_KeepsTheRowSelectedWhileMetadataWasLoading() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var repository = new PausedBackupList();
        var accessor = new TopLevelAccessor();
        await using var vm = new MainWindowViewModel(new InMemoryProfileRepository(), new InMemorySecretVault(),
            new DemoWorkspace(), new WindowDialogService(accessor), repository);
        await vm.InitializeAsync();
        await vm.RefreshBackupsCommand.ExecuteAsync();
        var page = new BackupsPage { DataContext = vm };
        var window = new Window { DataContext = vm, Content = page, Width = 1440, Height = 900 };
        window.Show();
        try
        {
            Dispatcher.UIThread.RunJobs();
            var list = page.GetVisualDescendants().OfType<ListBox>()
                .Single(control => ReferenceEquals(control.ItemsSource, vm.FilteredBackupMessages));
            list.SelectedItem = vm.BackupMessages[0];
            repository.PauseNextList = true;
            var refreshing = vm.RefreshBackupsCommand.ExecuteAsync();
            string expectedPath;
            try
            {
                await repository.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Dispatcher.UIThread.RunJobs();
                Assert.True(vm.IsBusy);
                Assert.True(list.IsEffectivelyEnabled);
                list.SelectedItem = vm.BackupMessages[1];
                Dispatcher.UIThread.RunJobs();
                expectedPath = ((BackupMessageItemViewModel)list.SelectedItem!).FilePath;
                Assert.Equal(expectedPath, vm.SelectedBackup?.FilePath);
            }
            finally
            {
                repository.Release.TrySetResult();
                await refreshing.WaitAsync(TimeSpan.FromSeconds(5));
            }
            Dispatcher.UIThread.RunJobs();
            Assert.Equal(expectedPath, vm.SelectedBackup?.FilePath);
            Assert.Equal(expectedPath, ((BackupMessageItemViewModel)list.SelectedItem!).FilePath);
            Assert.Null(vm.SelectedBackupMessage);
            Assert.Equal(0, repository.LoadCalls);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join("\n", BindingErrors.Instance.Messages));
        }
        finally { window.Close(); }
    });

    private sealed class PausedBackupList : IDeadLetterBackupRepository
    {
        public string RootDirectory => "Unused synthetic backup root";
        public bool PauseNextList { get; set; }
        public int LoadCalls { get; private set; }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default)
        {
            if (PauseNextList)
            {
                PauseNextList = false;
                Entered.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }
            return DemoData.Backups;
        }
        public Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            LoadCalls++;
            throw new InvalidOperationException("Metadata refresh must not load message bodies.");
        }
        public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("This selection test must not delete backups.");
    }
}
