using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class AwsSessionTokenEditorRegressionTests
{
    [Fact]
    public void EditingSavedKey_RejectsSessionTokenWithoutAccessKeyPair()
    {
        const string token = "synthetic-replacement-session-token";
        var editor = new ProfileEditorViewModel(SavedProfile())
        {
            AwsSessionToken = token
        };

        Assert.False(editor.TryBuild(out var result));

        Assert.Null(result);
        Assert.Contains("both the access key ID and the secret access key", editor.Error, StringComparison.Ordinal);
        Assert.DoesNotContain(token, editor.Error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void EditingSavedKey_WithBlankCredentialFieldsKeepsStoredCredentials(string token)
    {
        var saved = SavedProfile();
        var editor = new ProfileEditorViewModel(saved)
        {
            Name = "Synthetic AWS renamed",
            AwsSessionToken = token
        };

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.Equal(saved.Id, result!.Profile.Id);
        Assert.Equal("Synthetic AWS renamed", result.Profile.Name);
        Assert.Null(result.ConnectionString);
        Assert.False(result.ReplacesConnectionString);
    }

    [Theory]
    [InlineData("")]
    [InlineData("synthetic-new-session-token")]
    public void EditingSavedKey_WithCompleteKeyPairReplacesAllCredentialFields(string token)
    {
        var editor = new ProfileEditorViewModel(SavedProfile())
        {
            AwsAccessKeyId = "synthetic-access-key-id",
            AwsSecretAccessKey = "synthetic-secret-access-key",
            AwsSessionToken = token
        };

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.True(result!.ReplacesConnectionString);
        Assert.Equal(
            new AwsAccessKey("synthetic-access-key-id", "synthetic-secret-access-key",
                string.IsNullOrWhiteSpace(token) ? null : token),
            AwsAccessKey.Parse(result.ConnectionString));
    }

    private static ServiceBusProfile SavedProfile() =>
        ServiceBusProfile.CreateNew("Synthetic AWS", EnvironmentKind.Test,
            new AuthenticationSettings(AuthenticationKind.AwsAccessKey)) with
        {
            Provider = MessagingProvider.AmazonSqsSns,
            Aws = new AwsSettings("eu-west-1")
        };
}
