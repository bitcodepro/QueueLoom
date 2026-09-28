using System.Collections.Specialized;
using QueueLoom.App.Commands;
using QueueLoom.App.Models;

namespace QueueLoom.App.ViewModels;

/// <summary>View-only state: navigation shortcuts, Explorer ordering and empty-state flags.</summary>
public sealed partial class MainWindowViewModel
{
    private EntitySortColumn _entitySort = EntitySortColumn.Hierarchy;
    private bool _entitySortDescending;
    private int? _draftBodyErrorLine;

    public RelayCommand<string> NavigateCommand { get; private set; } = null!;

    public RelayCommand<string> SortEntitiesCommand { get; private set; } = null!;

    /// <summary>1-based line of the last JSON formatting error, for the editor to reveal.</summary>
    public int? DraftBodyErrorLine
    {
        get => _draftBodyErrorLine;
        private set => SetProperty(ref _draftBodyErrorLine, value);
    }

    public EntitySortColumn EntitySort
    {
        get => _entitySort;
        private set
        {
            if (SetProperty(ref _entitySort, value))
            {
                NotifyEntitySortChanged();
            }
        }
    }

    public bool EntitySortDescending
    {
        get => _entitySortDescending;
        private set
        {
            if (SetProperty(ref _entitySortDescending, value))
            {
                NotifyEntitySortChanged();
            }
        }
    }

    public bool IsSortedByHierarchy => EntitySort == EntitySortColumn.Hierarchy;
    public bool IsSortedByName => EntitySort == EntitySortColumn.Name;
    public bool IsSortedByStatus => EntitySort == EntitySortColumn.Status;
    public bool IsSortedByActive => EntitySort == EntitySortColumn.Active;
    public bool IsSortedByDeadLetters => EntitySort == EntitySortColumn.DeadLetters;
    public bool IsSortedByTransferDeadLetters => EntitySort == EntitySortColumn.TransferDeadLetters;
    public bool IsSortedByScheduled => EntitySort == EntitySortColumn.Scheduled;

    public string EntitySortDescription => EntitySort == EntitySortColumn.Hierarchy
        ? "Grouped by topic"
        : $"Sorted by {EntitySortLabel(EntitySort)} {(EntitySortDescending ? "↓" : "↑")}";

    public bool HasVisibleBackups => FilteredBackupMessages.Count > 0;

    public bool HasVisibleDlqSources => FilteredDeadLetterSources.Count > 0;

    public bool HasActivity => Activity.Count > 0;

    /// <summary>The "mcpServers" entry that makes Claude Desktop, Cursor and other MCP clients start QueueLoom.</summary>
    public string McpClientConfig { get; } = BuildMcpClientConfig();

    private static string BuildMcpClientConfig()
    {
        var process = Environment.ProcessPath ?? "QueueLoom.exe";
        var runsThroughDotnet = Path.GetFileNameWithoutExtension(process).Equals("dotnet", StringComparison.OrdinalIgnoreCase);
        string[] arguments = runsThroughDotnet
            ? [Path.Combine(AppContext.BaseDirectory, "QueueLoom.dll"), "--mcp"]
            : ["--mcp"];
        var server = new Dictionary<string, object>
        {
            ["command"] = process,
            ["args"] = arguments
        };
        return System.Text.Json.JsonSerializer.Serialize(
            new Dictionary<string, object> { ["mcpServers"] = new Dictionary<string, object> { ["queueloom"] = server } },
            new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
    }

    private void InitializePresentation()
    {
        NavigateCommand = new RelayCommand<string>(key =>
        {
            if (Enum.TryParse<NavigationPage>(key, out var page))
            {
                NavigateTo(page);
            }
        });
        SortEntitiesCommand = new RelayCommand<string>(column =>
        {
            if (!Enum.TryParse<EntitySortColumn>(column, out var sort))
            {
                return;
            }

            if (sort == EntitySortColumn.Hierarchy)
            {
                EntitySortDescending = false;
            }
            else if (sort == EntitySort)
            {
                EntitySortDescending = !EntitySortDescending;
            }
            else
            {
                // Counters are most useful largest-first; names read naturally A→Z.
                EntitySortDescending = sort is not (EntitySortColumn.Name or EntitySortColumn.Status);
            }

            EntitySort = sort;
            ApplyEntityFilter();
        });
        FilteredBackupMessages.CollectionChanged += OnFilteredBackupsChanged;
        FilteredDeadLetterSources.CollectionChanged += OnFilteredDlqSourcesChanged;
        Activity.CollectionChanged += OnActivityChanged;
    }

    private void OnFilteredBackupsChanged(object? sender, NotifyCollectionChangedEventArgs args)
    {
        OnPropertyChanged(nameof(HasVisibleBackups));
        OnPropertyChanged(nameof(DeleteVisibleBackupsLabel));
        DeleteVisibleBackupsCommand.NotifyCanExecuteChanged();
    }

    private void OnFilteredDlqSourcesChanged(object? sender, NotifyCollectionChangedEventArgs args) =>
        OnPropertyChanged(nameof(HasVisibleDlqSources));

    private void OnActivityChanged(object? sender, NotifyCollectionChangedEventArgs args) =>
        OnPropertyChanged(nameof(HasActivity));

    private IEnumerable<EntityItemViewModel> OrderEntities(IEnumerable<EntityItemViewModel> entities)
    {
        if (EntitySort == EntitySortColumn.Hierarchy)
        {
            return entities;
        }

        Func<EntityItemViewModel, IComparable> key = EntitySort switch
        {
            EntitySortColumn.Name => entity => entity.Reference.DisplayName,
            EntitySortColumn.Status => entity => entity.StatusLabel,
            EntitySortColumn.Active => entity => entity.Active,
            EntitySortColumn.DeadLetters => entity => entity.DeadLetters,
            EntitySortColumn.TransferDeadLetters => entity => entity.TransferDeadLetters,
            _ => entity => entity.Scheduled
        };
        var ordered = EntitySortDescending
            ? entities.OrderByDescending(key, Comparer<IComparable>.Create(CompareKeys))
            : entities.OrderBy(key, Comparer<IComparable>.Create(CompareKeys));
        return ordered.ThenBy(entity => entity.Reference.DisplayName, StringComparer.OrdinalIgnoreCase);
    }

    private static int CompareKeys(IComparable? left, IComparable? right) =>
        left is string leftText && right is string rightText
            ? StringComparer.OrdinalIgnoreCase.Compare(leftText, rightText)
            : Comparer<IComparable>.Default.Compare(left!, right!);

    private void NotifyEntitySortChanged()
    {
        OnPropertyChanged(nameof(IsSortedByHierarchy));
        OnPropertyChanged(nameof(IsSortedByName));
        OnPropertyChanged(nameof(IsSortedByStatus));
        OnPropertyChanged(nameof(IsSortedByActive));
        OnPropertyChanged(nameof(IsSortedByDeadLetters));
        OnPropertyChanged(nameof(IsSortedByTransferDeadLetters));
        OnPropertyChanged(nameof(IsSortedByScheduled));
        OnPropertyChanged(nameof(EntitySortDescription));
    }

    private static string EntitySortLabel(EntitySortColumn column) => column switch
    {
        EntitySortColumn.Name => "name",
        EntitySortColumn.Status => "status",
        EntitySortColumn.Active => "active messages",
        EntitySortColumn.DeadLetters => "dead letters",
        EntitySortColumn.TransferDeadLetters => "transfer dead letters",
        EntitySortColumn.Scheduled => "scheduled messages",
        _ => "hierarchy"
    };
}
