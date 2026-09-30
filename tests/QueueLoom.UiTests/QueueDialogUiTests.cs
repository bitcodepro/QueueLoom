using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class QueueDialogUiTests
{
    [Fact]
    public Task QueueDialog_ShowsOnlyWhatTheServiceSupports() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var azure = new QueueManagementCapabilities("queue",
            QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration | QueueSettingFlags.DeadLetterOnExpiration,
            QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration | QueueSettingFlags.DeadLetterOnExpiration,
            CanCreateDeadLetterQueue: false);
        var create = new QueueDialogWindow(new QueueDialogViewModel(azure, "Payments · Prod") { Name = "refunds", TimeToLive = 14, MaxDeliveryCount = 10 });
        create.Show();
        Assert.Contains("Create queue", Buttons(create));
        Assert.DoesNotContain(create.GetVisualDescendants().OfType<CheckBox>(), box => box.IsEffectivelyVisible && (box.Content as string)?.StartsWith("Also create", StringComparison.Ordinal) == true);
        await Save(create, "queue-dialog-new.png");
        create.Close();

        var rabbit = new QueueManagementCapabilities("queue", QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount,
            QueueSettingFlags.None, CanCreateDeadLetterQueue: true, UpdateNote: "RabbitMQ does not change a queue's settings after it is created.");
        var kafka = new QueueManagementCapabilities("topic", QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions,
            QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions, CanCreateDeadLetterQueue: true, UpdateNote: "Partitions can be added but never removed.");
        var edit = new QueueDialogWindow(new QueueDialogViewModel(kafka, "Events", "shipments", new QueueSettings(TimeSpan.FromDays(7), Partitions: 6)));
        edit.Show();
        Assert.Contains("Save changes", Buttons(edit));
        Assert.Contains(edit.GetVisualDescendants().OfType<TextBlock>(), text => text.IsEffectivelyVisible && text.Text == "Partitions can be added but never removed.");
        await Save(edit, "queue-dialog-kafka-settings.png");
        edit.Close();

        var rabbitNew = new QueueDialogWindow(new QueueDialogViewModel(rabbit, "Billing") { Name = "refunds" });
        rabbitNew.Show();
        Assert.Contains(rabbitNew.GetVisualDescendants().OfType<CheckBox>(), box => box.IsEffectivelyVisible && box.IsChecked == true);
        rabbitNew.Close();
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    private static string[] Buttons(Window window)
    {
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        return window.GetVisualDescendants().OfType<Button>().Where(button => button.IsEffectivelyVisible)
            .Select(button => button.Content as string ?? string.Empty).ToArray();
    }

    private static async Task Save(Window window, string name)
    {
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        using var frame = window.CaptureRenderedFrame();
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (!string.IsNullOrWhiteSpace(directory) && frame is not null)
        {
            Directory.CreateDirectory(directory);
            await using var file = File.Create(Path.Combine(directory, name));
            frame.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
        }
    }
}
