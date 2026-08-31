using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using System.Xml.Linq;

namespace QueueLoom.Tests;

public sealed class UiPresentationTests
{
    [Fact]
    public void WindowsManifest_EnablesLongPathAwareness()
    {
        var repositoryRoot = FindRepositoryRoot();
        var manifest = XDocument.Load(Path.Combine(repositoryRoot, "src", "QueueLoom.App", "app.manifest"));
        var longPathAware = Assert.Single(
            manifest.Descendants(),
            element => element.Name.LocalName == "longPathAware");

        Assert.Equal("true", longPathAware.Value.Trim(), ignoreCase: true);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 18)]
    [InlineData(2, 36)]
    public void EntityIndentMargin_ProducesVisibleHierarchy(int indent, double expectedLeft)
    {
        var item = new EntityItemViewModel(
            ServiceBusEntityReference.Subscription("orders", "processor"),
            ServiceBusEntityRuntime.Empty,
            ServiceBusEntityStatus.Active,
            requiresSession: false,
            indent);

        Assert.Equal(expectedLeft, item.IndentMargin.Left);
        Assert.Equal(0, item.IndentMargin.Top);
        Assert.Equal(0, item.IndentMargin.Right);
        Assert.Equal(0, item.IndentMargin.Bottom);
    }

    [Fact]
    public void EntityIndent_RejectsNegativeLevels()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new EntityItemViewModel(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusEntityRuntime.Empty,
            ServiceBusEntityStatus.Active,
            requiresSession: false,
            indent: -1));
    }

    [Fact]
    public void ConfirmDialog_UsesExplicitCloseLabelForErrorPresentation()
    {
        var dialog = new ConfirmDialogViewModel(
            "Connection failed",
            "The environment could not be reached.",
            isDangerous: false,
            requiredText: null,
            showCancel: false,
            confirmLabel: "Close");

        Assert.Equal("Close", dialog.ConfirmLabel);
        Assert.False(dialog.ShowCancel);
        Assert.False(dialog.IsDangerous);
        Assert.True(dialog.CanConfirm);
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "QueueLoom.slnx")))
            {
                return directory.FullName;
            }
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not find the QueueLoom repository root.");
    }
}
