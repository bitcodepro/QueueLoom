using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Profiles;

namespace QueueLoom.UiTests;

public sealed class AzureSasNamespaceUiTests
{
    [Fact]
    public Task SasEntraSasTransition_SaveKeepsNamespaceOfRetainedSecret() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var original = ServiceBusProfile.CreateNew("SAS", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), "sas-a.servicebus.windows.net");
        var editor = new ProfileEditorViewModel(original);
        var owner = new Window();
        owner.Show();
        var window = new ProfileEditorWindow(editor);
        var saved = window.ShowDialog<ProfileEditorResult?>(owner);
        try
        {
            Dispatcher.UIThread.RunJobs();
            var authentication = window.GetVisualDescendants().OfType<ComboBox>()
                .Single(control => AutomationProperties.GetName(control) == "Sign-in method");
            authentication.SelectedItem = editor.AuthenticationOptions.Single(option => option.Kind == AuthenticationKind.EntraId);
            Dispatcher.UIThread.RunJobs();
            var namespaceBox = window.GetVisualDescendants().OfType<TextBox>()
                .Single(control => control.PlaceholderText == "contoso.servicebus.windows.net");
            Assert.True(namespaceBox.IsEffectivelyVisible);
            namespaceBox.Text = "sas-b.servicebus.windows.net";
            Dispatcher.UIThread.RunJobs();
            Assert.Equal("sas-b.servicebus.windows.net", editor.FullyQualifiedNamespace);
            authentication.SelectedItem = editor.AuthenticationOptions.Single(option => option.Kind == AuthenticationKind.ConnectionString);
            Dispatcher.UIThread.RunJobs();
            Assert.False(namespaceBox.IsEffectivelyVisible);
            Assert.Equal(string.Empty, editor.ConnectionString);

            window.GetVisualDescendants().OfType<Button>().Single(button => Equals(button.Content, "Save environment"))
                .RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
            var result = Assert.IsType<ProfileEditorResult>(await saved);
            Assert.Equal(original.FullyQualifiedNamespace, result.Profile.FullyQualifiedNamespace);
            Assert.Null(result.ConnectionString);
            Assert.False(result.ReplacesConnectionString);
            Assert.Empty(BindingErrors.Instance.Messages);
        }
        finally
        {
            window.Close();
            owner.Close();
        }
    });
}
