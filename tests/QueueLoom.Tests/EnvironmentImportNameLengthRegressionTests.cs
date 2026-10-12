using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

public sealed class EnvironmentImportNameLengthRegressionTests
{
    [Theory]
    [InlineData(96)]
    [InlineData(97)]
    [InlineData(98)]
    [InlineData(99)]
    [InlineData(100)]
    public void ImportName_ValidExportWithCollidingLongNameIsImported(int length)
    {
        var source = Profile(new string('a', length));
        Assert.True(ProfileValidator.Validate(source).IsValid);
        var import = EnvironmentTransfer.Import(EnvironmentTransfer.Export([source]), [source]);

        Assert.Empty(import.Skipped);
        var imported = Assert.Single(import.Profiles);
        Assert.True(ProfileValidator.Validate(imported).IsValid);
        Assert.Equal(new string('a', Math.Min(length, ProfileValidator.MaxNameLength - 4)) + " (2)", imported.Name);
        Assert.NotEqual(source.Id, imported.Id);
        Assert.Equal(ProfileAccessMode.ReadOnly, imported.AccessMode);
    }

    [Fact]
    public void ImportName_ManyCaseInsensitiveCollisionsReserveTheWholeSuffix()
    {
        var source = Profile(new string('a', ProfileValidator.MaxNameLength));
        var existing = new List<ServiceBusProfile> { source with { Name = source.Name.ToUpperInvariant() } };
        for (var number = 2; number <= 125; number++)
        {
            var suffix = $" ({number})";
            var name = new string('a', ProfileValidator.MaxNameLength - suffix.Length) + suffix;
            existing.Add(Profile(number % 2 == 0 ? name.ToUpperInvariant() : name));
        }
        var import = EnvironmentTransfer.Import(EnvironmentTransfer.Export([source]), existing);

        Assert.Empty(import.Skipped);
        var imported = Assert.Single(import.Profiles);
        Assert.Equal(new string('a', 94) + " (126)", imported.Name);
        Assert.True(ProfileValidator.Validate(imported).IsValid);
        Assert.DoesNotContain(existing, item => string.Equals(item.Name, imported.Name, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ImportName_CollisionsWithinOneFileRemainUnique()
    {
        var source = Profile(new string('a', ProfileValidator.MaxNameLength));
        var import = EnvironmentTransfer.Import(EnvironmentTransfer.Export([source, source with { Name = source.Name.ToUpperInvariant() }, source]), []);
        Assert.Empty(import.Skipped);
        Assert.Equal(3, import.Profiles.Count);
        Assert.Equal(3, import.Profiles.Select(profile => profile.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.All(import.Profiles, profile => Assert.True(ProfileValidator.Validate(profile).IsValid));
    }

    [Fact]
    public void ImportName_SuffixTruncationDoesNotSplitASurrogatePair()
    {
        var source = Profile(new string('a', 95) + "😀" + "bbb");
        Assert.Equal(ProfileValidator.MaxNameLength, source.Name.Length);
        var import = EnvironmentTransfer.Import(EnvironmentTransfer.Export([source]), [source]);
        Assert.Empty(import.Skipped);
        var imported = Assert.Single(import.Profiles);
        Assert.Equal(new string('a', 95) + " (2)", imported.Name);
        Assert.DoesNotContain(imported.Name, char.IsSurrogate);
    }

    [Fact]
    public void ImportName_NoncollidingMaximumLengthNameIsUnchanged()
    {
        var source = Profile(new string('a', ProfileValidator.MaxNameLength));
        var imported = Assert.Single(EnvironmentTransfer.Import(EnvironmentTransfer.Export([source]), []).Profiles);
        Assert.Equal(source.Name, imported.Name);
    }

    [Fact]
    public void ImportName_AlreadyInvalidOverlongInputRemainsRejectedEvenWithACollision()
    {
        var invalid = Profile(new string('a', ProfileValidator.MaxNameLength + 1));
        var import = EnvironmentTransfer.Import(EnvironmentTransfer.Export([invalid]), [invalid]);
        Assert.Empty(import.Profiles);
        Assert.Single(import.Skipped);
    }

    private static ServiceBusProfile Profile(string name) => ViewModelStateTests.CreateProfile(name, EnvironmentKind.Test, ProfileAccessMode.ReadWrite)
        with { FullyQualifiedNamespace = "synthetic.servicebus.windows.net" };
}
