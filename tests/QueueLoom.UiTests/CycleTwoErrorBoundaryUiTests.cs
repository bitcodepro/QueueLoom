using System.Text.RegularExpressions;
using Avalonia.Threading;
using Microsoft.Extensions.Logging;
using QueueLoom.App.ViewModels;

namespace QueueLoom.UiTests;

public sealed class CycleTwoErrorBoundaryUiTests
{
    [Fact]
    public Task CycleTwoLogging_ActualAsyncCommandContainsLoggerFailureAndShowsOriginalError() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenWithLoggerAsync(new ThrowingLogger());
        var vm = fixture.ViewModel;
        vm.DeadLetterSearchQuery = "$.missing ==";
        var errors = new List<Exception>();
        void Handle(object? sender, DispatcherUnhandledExceptionEventArgs e) { errors.Add(e.Exception); e.Handled = true; }
        Dispatcher.UIThread.UnhandledException += Handle;
        try
        {
            vm.SearchDeadLettersCommand.Execute(null);
            // async-void command posts its failure to the actual dispatcher; drain it before inspecting.
            try { await vm.SearchDeadLettersCommand.Completion; } catch { }
            await fixture.SettleAsync();
            Assert.Empty(errors);
            Assert.False(vm.IsBusy);
            Assert.True(vm.HasError);
            Assert.Contains("A value is missing", vm.ErrorText, StringComparison.Ordinal);
        }
        finally { Dispatcher.UIThread.UnhandledException -= Handle; }
    });

    private sealed class ThrowingLogger : ILogger<MainWindowViewModel>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { if (level == LogLevel.Error) throw new RegexMatchTimeoutException(); }
    }
}
