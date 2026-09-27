using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.Controls;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;

namespace QueueLoom.UiTests;

public sealed class CloudProviderUiTests
{
    [Fact]
    public Task Environments_page_shows_the_cloud_of_every_environment() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(allClouds: true);

        await fixture.NavigateAsync("Environments");
        fixture.Window.CaptureRenderedFrame();

        var page = fixture.Window.GetVisualDescendants().OfType<QueueLoom.App.Views.Pages.EnvironmentsPage>().Single();
        var badges = page.GetVisualDescendants().OfType<ProviderBadge>().Where(badge => badge.IsVisible).ToArray();
        Assert.Equal(
            [MessagingProvider.AzureServiceBus, MessagingProvider.AzureServiceBus, MessagingProvider.AmazonSqsSns, MessagingProvider.GooglePubSub],
            badges.Select(badge => badge.Provider!.Value).Order());
        Assert.Contains(page.GetVisualDescendants().OfType<TextBlock>(), text => text.Text == "Amazon SQS / SNS");
        Assert.Contains(page.GetVisualDescendants().OfType<TextBlock>(), text => text.Text == "shipping-dev-2231");
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    [Fact]
    public Task Top_bar_shows_the_cloud_and_region_of_the_connected_environment() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(allClouds: true);

        fixture.ViewModel.SelectedProfile = fixture.ViewModel.Profiles.Single(profile => profile.Provider == MessagingProvider.AmazonSqsSns);
        await fixture.ViewModel.ConnectCommand.ExecuteAsync();
        await fixture.SettleAsync();
        fixture.Window.CaptureRenderedFrame();

        Assert.Equal(MessagingProvider.AmazonSqsSns, fixture.ViewModel.ConnectedProvider);
        Assert.Equal("eu-central-1", fixture.ViewModel.ConnectedNamespace);
        Assert.Equal("Receive and release", fixture.ViewModel.BrowseModeLabel);
        var topBarBadge = fixture.Window.GetVisualDescendants().OfType<ProviderBadge>()
            .Single(badge => badge.IsEffectivelyVisible && badge.FindAncestorOfType<ComboBox>() is null &&
                             badge.FindAncestorOfType<QueueLoom.App.Views.Pages.OverviewPage>() is null);
        Assert.Equal(MessagingProvider.AmazonSqsSns, topBarBadge.Provider);
        Assert.Contains(topBarBadge.GetVisualDescendants().OfType<TextBlock>(), text => text.Text == "AWS");
    });

    [Fact]
    public Task Editor_shows_the_fields_of_the_chosen_cloud() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var viewModel = new ProfileEditorViewModel(null);
        var window = new ProfileEditorWindow(viewModel) { Width = 700, Height = 900 };
        window.Show();
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();

        Assert.True(Visible(window, "Passwordless Entra ID"));
        Assert.False(Visible(window, "AWS account"));

        viewModel.SelectedProvider = viewModel.ProviderOptions.Single(option => option.Provider == MessagingProvider.AmazonSqsSns);
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        window.CaptureRenderedFrame();
        Assert.True(Visible(window, "AWS account"));
        Assert.False(Visible(window, "Passwordless Entra ID"));
        Assert.Equal(AuthenticationKind.AwsAccessKey, viewModel.AuthenticationKind);

        viewModel.SelectedProvider = viewModel.ProviderOptions.Single(option => option.Provider == MessagingProvider.GooglePubSub);
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        window.CaptureRenderedFrame();
        Assert.True(Visible(window, "Google Cloud project"));
        Assert.False(Visible(window, "AWS account"));

        window.Close();
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
        await Task.CompletedTask;
    });

    [Fact]
    public Task Screenshots_of_clouds_are_written_when_requested() => UiSession.RunAsync(async () =>
    {
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
        await using var fixture = await WindowFixture.OpenAsync(allClouds: true);
        foreach (var theme in new[] { AppThemePreference.Dark, AppThemePreference.Light })
        {
            fixture.ViewModel.ThemePreference = theme;
            await fixture.NavigateAsync("Environments");
            Save(fixture.Window, Path.Combine(directory, $"{theme.ToString().ToLowerInvariant()}-environments-clouds.png"));

            var viewModel = new ProfileEditorViewModel(null) { Name = "Payments on AWS", Environment = EnvironmentKind.Test };
            viewModel.SelectedProvider = viewModel.ProviderOptions.Single(option => option.Provider == MessagingProvider.AmazonSqsSns);
            viewModel.AwsRegion = "eu-central-1";
            viewModel.AwsAccessKeyId = "AKIAIOSFODNN7EXAMPLE";
            viewModel.AwsSecretAccessKey = "example";
            var editor = new ProfileEditorWindow(viewModel) { Width = 700, Height = 860 };
            editor.Show();
            await fixture.SettleAsync();
            Save(editor, Path.Combine(directory, $"{theme.ToString().ToLowerInvariant()}-editor-aws.png"));
            editor.Close();
        }
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
    });

    private static bool Visible(Window window, string text) =>
        window.GetVisualDescendants().OfType<TextBlock>().Any(block => block.Text == text && block.IsEffectivelyVisible);

    private static void Save(TopLevel window, string path)
    {
        using var frame = window.CaptureRenderedFrame();
        using var file = File.Create(path);
        frame?.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
