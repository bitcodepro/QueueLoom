using QueueLoom.Core.Updates;
using Xunit;

namespace QueueLoom.Tests;

public sealed class InstallerVersionTests
{
    [Theory]
    [InlineData("1.2.4", "1.2.3", 1)]
    [InlineData("1.2.3", "1.2.3-rc.99", 1)]
    [InlineData("1.2.3-rc.10", "1.2.3-rc.9", 1)]
    [InlineData("1.2.3-rc.1", "1.2.3-rc", 1)]
    [InlineData("1.2.3-rc.1", "1.2.3", -1)]
    [InlineData("1.2.3", "1.2.3", 0)]
    public void InstallerCannotDowngradeAnExistingPayload(string candidate, string active, int expected)
        => Assert.Equal(expected, Math.Sign(VersionInstallation.CompareReleaseVersions(candidate, active)));
}
