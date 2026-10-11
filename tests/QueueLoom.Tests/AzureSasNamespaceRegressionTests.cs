using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private const string SasNamespaceA = "sas-a.servicebus.windows.net";
    private const string SasNamespaceB = "sas-b.servicebus.windows.net";
    private const string SyntheticSasA = "Endpoint=sb://sas-a.servicebus.windows.net/;SharedAccessKeyName=TestOnly;SharedAccessKey=c3ludGhldGlj;";
    private const string SyntheticSasB = "Endpoint=sb://sas-b.servicebus.windows.net/;SharedAccessKeyName=TestOnly;SharedAccessKey=c3ludGhldGlj;";

    [Theory]
    [InlineData(SasNamespaceA)]
    [InlineData(null)]
    [InlineData("localhost")]
    public void AzureSasNamespace_BlankSecretAfterEntraRoundTripKeepsSavedNamespace(string? savedNamespace)
    {
        var original = CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = savedNamespace };
        var editor = SasEditorAfterEntraRoundTrip(original);

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(savedNamespace, result!.Profile.FullyQualifiedNamespace);
        Assert.Equal(AuthenticationKind.ConnectionString, result.Profile.Authentication.Kind);
        Assert.Null(result.ConnectionString);
        Assert.False(result.ReplacesConnectionString);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AzureSasNamespace_EditCommandPersistsMatchingNamespaceAndSecret(bool replaceSecret)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var repository = new JsonProfileRepository(paths);
        var original = CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = SasNamespaceA };
        await repository.UpsertAndSelectAsync(original);
        using var vault = new EncryptedFileSecretVault(paths, new SyntheticMasterKeyStore());
        var secretKey = ProfileSecretKey.ConnectionString(original.Id);
        await vault.StoreAsync(secretKey, SyntheticSasA);
        var editor = SasEditorAfterEntraRoundTrip(original);
        editor.Name = "Renamed SAS";
        if (replaceSecret)
        {
            editor.FullyQualifiedNamespace = "stale.servicebus.windows.net";
            editor.ConnectionString = SyntheticSasB;
        }
        Assert.True(editor.TryBuild(out var result), editor.Error);
        await using var viewModel = CreateViewModel(repository, new FakeWorkspace(),
            new FakeDialogService { EditResult = result }, secretVault: vault);
        await viewModel.InitializeAsync();

        await viewModel.EditEnvironmentCommand.ExecuteAsync();

        Assert.False(viewModel.HasError, viewModel.ErrorText);
        // Read through a fresh repository instance, so this checks the actual saved metadata.
        using var reader = new JsonProfileRepository(paths);
        var saved = Assert.IsType<ServiceBusProfile>(await reader.GetAsync(original.Id));
        Assert.Equal(replaceSecret ? SyntheticSasB : SyntheticSasA,
            await vault.RetrieveForProfileAsync(saved, ProfileSecretKind.ConnectionString));
        Assert.Equal(replaceSecret ? SasNamespaceB : SasNamespaceA, saved.FullyQualifiedNamespace);
        Assert.Equal(replaceSecret ? SyntheticSasB : SyntheticSasA, await vault.RetrieveAsync(secretKey));
        Assert.Equal("Renamed SAS", saved.Name);
        Assert.Equal(AuthenticationKind.ConnectionString, saved.Authentication.Kind);
        Assert.NotEqual(original.ConfigurationRevision, saved.ConfigurationRevision);
        Assert.Equal(original.Id, await reader.GetSelectedProfileIdAsync());
    }

    [Fact]
    public void AzureSasNamespace_OrdinaryBlankSecretEditKeepsSavedNamespaceAndOtherEdits()
    {
        var original = CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = SasNamespaceA };
        var editor = new ProfileEditorViewModel(original) { Name = "Renamed", AllowQueueManagement = true };

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(SasNamespaceA, result!.Profile.FullyQualifiedNamespace);
        Assert.Equal("Renamed", result.Profile.Name);
        Assert.True(result.Profile.AllowQueueManagement);
        Assert.Null(result.ConnectionString);
        Assert.False(result.ReplacesConnectionString);
    }

    [Fact]
    public void AzureSasNamespace_EntraEditStillUsesEditedNamespace()
    {
        var original = CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = SasNamespaceA };
        var editor = new ProfileEditorViewModel(original)
        {
            AuthenticationKind = AuthenticationKind.EntraId,
            FullyQualifiedNamespace = SasNamespaceB
        };

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(SasNamespaceB, result!.Profile.FullyQualifiedNamespace);
        Assert.Equal(AuthenticationKind.EntraId, result.Profile.Authentication.Kind);
        Assert.Null(result.ConnectionString);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AzureSasNamespace_NewOrExistingEntraProfileCannotKeepANonexistentSasSecret(bool existing)
    {
        var original = existing ? CreateProfile("Entra", EnvironmentKind.Development) : null;
        var editor = new ProfileEditorViewModel(original)
            { Name = "SAS", AuthenticationKind = AuthenticationKind.ConnectionString };

        Assert.False(editor.TryBuild(out var result));
        Assert.Null(result);
        Assert.Contains("Enter a namespace-level", editor.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureSasNamespace_InvalidReplacementDoesNotFallBackToRetainedSecret()
    {
        var original = CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = SasNamespaceA };
        var editor = SasEditorAfterEntraRoundTrip(original);
        editor.ConnectionString = "invalid-synthetic-replacement";

        Assert.False(editor.TryBuild(out var result));
        Assert.Null(result);
        Assert.True(editor.HasError);
    }

    private static ProfileEditorViewModel SasEditorAfterEntraRoundTrip(ServiceBusProfile original)
    {
        var editor = new ProfileEditorViewModel(original);
        editor.AuthenticationKind = AuthenticationKind.EntraId;
        editor.FullyQualifiedNamespace = SasNamespaceB;
        editor.AuthenticationKind = AuthenticationKind.ConnectionString;
        Assert.True(editor.HasExistingConnectionString);
        Assert.Equal(string.Empty, editor.ConnectionString);
        return editor;
    }
}
