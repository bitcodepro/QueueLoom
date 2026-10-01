using Confluent.Kafka;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class SettingsAuditRegressionTests
{
    [Fact]
    public async Task ProtobufSchemaPath_SurvivesRestartAndUnrelatedPreferenceUpdate()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        const string schema = "C:/isolated-fixtures/orders.proto";
        using (var store = new JsonAppSettingsStore(paths))
            await store.UpdateAsync(settings => settings with { ProtobufSchemaPath = schema });
        using (var reopened = new JsonAppSettingsStore(paths))
        {
            Assert.Equal(schema, (await reopened.LoadAsync()).ProtobufSchemaPath);
            await reopened.SaveThemeAsync(AppThemePreference.Light);
        }
        using var final = new JsonAppSettingsStore(paths);
        Assert.Equal(schema, (await final.LoadAsync()).ProtobufSchemaPath);
    }

}
