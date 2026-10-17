using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Composer templates: a message saved by name, to start new messages from.</summary>
public sealed partial class MainWindowViewModel
{
    private IComposerTemplateStore? _templates;
    private ComposerTemplate? _selectedComposerTemplate;
    private string _templateName = string.Empty;
    // Loading, saving and deleting each change or read the store and then publish its list. They run one at a time,
    // so an older list cannot land after a newer change and bring back a deleted template.
    private readonly SemaphoreSlim _templateGate = new(1, 1);

    public ObservableCollection<ComposerTemplate> ComposerTemplates { get; } = [];

    public bool HasComposerTemplates => ComposerTemplates.Count > 0;

    public bool SupportsComposerTemplates => _templates is not null;

    public ComposerTemplate? SelectedComposerTemplate
    {
        get => _selectedComposerTemplate;
        set
        {
            if (SetProperty(ref _selectedComposerTemplate, value))
            {
                if (value is not null) TemplateName = value.Name;
                UseComposerTemplateCommand.NotifyCanExecuteChanged();
                DeleteComposerTemplateCommand.NotifyCanExecuteChanged();
            }
        }
    }

    /// <summary>The name the current draft is saved under.</summary>
    public string TemplateName
    {
        get => _templateName;
        set
        {
            if (SetProperty(ref _templateName, value ?? string.Empty)) SaveComposerTemplateCommand.NotifyCanExecuteChanged();
        }
    }

    public AsyncRelayCommand SaveComposerTemplateCommand { get; private set; } = null!;

    public RelayCommand UseComposerTemplateCommand { get; private set; } = null!;

    public AsyncRelayCommand DeleteComposerTemplateCommand { get; private set; } = null!;

    private void InitializeComposerTemplates(IComposerTemplateStore? templates)
    {
        _templates = templates;
        SaveComposerTemplateCommand = _commands.Create(SaveComposerTemplateAsync,
            () => _templates is not null && !string.IsNullOrWhiteSpace(TemplateName));
        UseComposerTemplateCommand = new RelayCommand(UseSelectedComposerTemplate,
            () => !IsBusy && IsConnected && SelectedComposerTemplate is not null);
        DeleteComposerTemplateCommand = _commands.Create(DeleteSelectedComposerTemplateAsync,
            () => _templates is not null && SelectedComposerTemplate is not null);
        ComposerTemplates.CollectionChanged += (_, _) => OnPropertyChanged(nameof(HasComposerTemplates));
    }

    /// <summary>Reads the saved templates; a failure is shown and leaves the list as it was.</summary>
    internal async Task LoadComposerTemplatesAsync(CancellationToken cancellationToken = default)
    {
        if (_templates is null) return;
        await EnterTemplatesAsync().ConfigureAwait(true);
        try
        {
            var store = _templates;
            var templates = await Task.Run(() => store.ListAsync(cancellationToken), cancellationToken).ConfigureAwait(true);
            ReplaceComposerTemplates(templates, SelectedComposerTemplate?.Name);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            ErrorText = $"Templates could not be read: {SanitizeException(exception)}";
        }
        finally
        {
            _templateGate.Release();
        }
    }

    private async Task SaveComposerTemplateAsync(CancellationToken cancellationToken)
    {
        try
        {
            var template = new ComposerTemplate(
                ComposerTemplate.NormalizeName(TemplateName),
                DraftBody,
                DraftBodyFormat,
                DraftContentType,
                DraftSubject,
                DraftCorrelationId,
                DraftTo,
                DraftReplyTo,
                DraftSessionId,
                DraftReplyToSessionId,
                DraftPartitionKey,
                DraftTimeToLiveSeconds,
                DraftApplicationProperties,
                DraftTransactionPartitionKey);
            var store = _templates ?? throw new InvalidOperationException("Templates are not available here.");
            await EnterTemplatesAsync().ConfigureAwait(true);
            try
            {
                await Task.Run(() => store.SaveAsync(template, cancellationToken), cancellationToken).ConfigureAwait(true);
                var templates = await Task.Run(() => store.ListAsync(cancellationToken), cancellationToken).ConfigureAwait(true);
                ReplaceComposerTemplates(templates, template.Name);
                StatusText = $"Template '{template.Name}' saved.";
            }
            finally
            {
                _templateGate.Release();
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            ErrorText = SanitizeException(exception);
        }
    }

    private async Task DeleteSelectedComposerTemplateAsync(CancellationToken cancellationToken)
    {
        var template = SelectedComposerTemplate;
        var store = _templates;
        if (template is null || store is null) return;
        if (!await _dialogs.ConfirmAsync("Delete template", $"Delete the template '{template.Name}'? Messages sent from it are not affected.",
                false, cancellationToken: cancellationToken).ConfigureAwait(true)) return;
        // The confirmation is asked before the gate: an open dialog does not hold up the other operations.
        await EnterTemplatesAsync().ConfigureAwait(true);
        try
        {
            await Task.Run(() => store.DeleteAsync(template.Name, cancellationToken), cancellationToken).ConfigureAwait(true);
            var templates = await Task.Run(() => store.ListAsync(cancellationToken), cancellationToken).ConfigureAwait(true);
            ReplaceComposerTemplates(templates, null);
            StatusText = $"Template '{template.Name}' deleted.";
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            ErrorText = SanitizeException(exception);
        }
        finally
        {
            _templateGate.Release();
        }
    }

    /// <summary>
    /// Starts a new message from the selected template, in the connected environment. It gets a new Message ID and no
    /// scheduled time, like a new message.
    /// </summary>
    private void UseSelectedComposerTemplate()
    {
        var template = SelectedComposerTemplate ?? throw new InvalidOperationException("Choose a template first.");
        NewMessage();
        DraftBody = template.Body ?? string.Empty;
        DraftBodyFormat = template.BodyFormat;
        DraftContentType = template.ContentType ?? string.Empty;
        DraftSubject = template.Subject ?? string.Empty;
        DraftCorrelationId = template.CorrelationId ?? string.Empty;
        DraftTo = template.To ?? string.Empty;
        DraftReplyTo = template.ReplyTo ?? string.Empty;
        DraftSessionId = template.SessionId ?? string.Empty;
        DraftReplyToSessionId = template.ReplyToSessionId ?? string.Empty;
        DraftPartitionKey = template.PartitionKey ?? string.Empty;
        DraftTransactionPartitionKey = template.TransactionPartitionKey ?? string.Empty;
        DraftTimeToLiveSeconds = template.TimeToLiveSeconds ?? string.Empty;
        DraftApplicationProperties = string.IsNullOrWhiteSpace(template.ApplicationProperties) ? "{}" : template.ApplicationProperties;
        DraftOriginNotice = $"New message from template '{template.Name}' · it has a new Message ID; " +
                            "the destination is pinned to the connected environment";
    }

    private int _templateWaiters;

    /// <summary>Operations waiting for, or just taking, the template gate (tests use it to order overlaps).</summary>
    internal int ComposerTemplateWaiters => Volatile.Read(ref _templateWaiters);

    private async Task EnterTemplatesAsync()
    {
        Interlocked.Increment(ref _templateWaiters);
        try { await _templateGate.WaitAsync(CancellationToken.None).ConfigureAwait(true); }
        finally { Interlocked.Decrement(ref _templateWaiters); }
    }

    private void ReplaceComposerTemplates(IReadOnlyList<ComposerTemplate> templates, string? select)
    {
        ComposerTemplates.Clear();
        foreach (var template in templates) ComposerTemplates.Add(template);
        SelectedComposerTemplate = select is null
            ? null
            : ComposerTemplates.FirstOrDefault(template => string.Equals(template.Name, select, StringComparison.OrdinalIgnoreCase));
    }
}
