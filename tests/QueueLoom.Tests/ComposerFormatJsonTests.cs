using System.Text.Json;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // "Format JSON" in the composer must only re-indent: the text that is sent must keep the characters the operator
    // typed (Cyrillic, accents, <, >, &, ', +), and numbers keep their spelling.
    [Fact]
    public async Task FormatJson_OnlyReindentsAndKeepsTheTypedCharacters()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace());
        const string original = "{\"name\":\"Ёлка café\",\"html\":\"<b>a&b</b> it's +1\",\"price\":1.0,\"id\":12345678901234567890}";
        vm.DraftBody = original;

        vm.FormatJsonCommand.Execute(null);

        Assert.True(string.IsNullOrEmpty(vm.ErrorText), vm.ErrorText);
        Assert.Contains('\n', vm.DraftBody);
        Assert.Contains("\"Ёлка café\"", vm.DraftBody, StringComparison.Ordinal);
        Assert.Contains("\"<b>a&b</b> it's +1\"", vm.DraftBody, StringComparison.Ordinal);
        Assert.Contains("1.0", vm.DraftBody, StringComparison.Ordinal);
        Assert.Contains("12345678901234567890", vm.DraftBody, StringComparison.Ordinal);
        using var before = JsonDocument.Parse(original);
        using var after = JsonDocument.Parse(vm.DraftBody);
        Assert.True(JsonElement.DeepEquals(before.RootElement, after.RootElement));
    }
}
