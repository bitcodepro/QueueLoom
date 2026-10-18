using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

// An environments file without a version, or with version 0, was refused as "written by a newer QueueLoom", which sent
// the operator to update for nothing; a version or format written as text threw the JSON library's own message. Each
// case now says what is wrong with the file, and only a higher version asks for an update.
public sealed class EnvironmentImportVersionTests
{
    private static string File(Action<JsonObject> change)
    {
        var profile = ServiceBusProfile.CreateNew("Local", EnvironmentKind.Test, AuthenticationSettings.ConnectionString(), "local.servicebus.windows.net");
        var root = JsonNode.Parse(EnvironmentTransfer.Export([profile]))!.AsObject();
        change(root);
        return root.ToJsonString();
    }

    private static string Refusal(Action<JsonObject> change) =>
        Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Import(File(change), [])).Message;

    [Fact]
    public void AHigherVersionAsksForAnUpdate() =>
        Assert.Contains("newer QueueLoom", Refusal(root => root["version"] = EnvironmentTransfer.Version + 1), StringComparison.Ordinal);

    [Theory]
    [InlineData("missing")]
    [InlineData("zero")]
    [InlineData("negative")]
    [InlineData("text")]
    [InlineData("fraction")]
    public void AnInvalidVersionSaysSoInsteadOfAskingForAnUpdate(string version)
    {
        var message = Refusal(root =>
        {
            switch (version)
            {
                case "missing": root.Remove("version"); break;
                case "zero": root["version"] = 0; break;
                case "negative": root["version"] = -1; break;
                case "text": root["version"] = "1"; break;
                case "fraction": root["version"] = 1.5; break;
            }
        });

        Assert.DoesNotContain("newer", message, StringComparison.Ordinal);
        Assert.Contains("version", message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AFormatWrittenAsANumberIsNotAnEnvironmentsFile() =>
        Assert.Contains("not a QueueLoom environments file", Refusal(root => root["format"] = 7), StringComparison.Ordinal);

    [Fact]
    public void TheCurrentVersionStillImports() =>
        Assert.Single(EnvironmentTransfer.Import(File(_ => { }), []).Profiles);
}
