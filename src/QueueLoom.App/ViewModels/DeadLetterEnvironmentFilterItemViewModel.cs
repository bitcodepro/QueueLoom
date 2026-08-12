namespace QueueLoom.App.ViewModels;

public sealed class DeadLetterEnvironmentFilterItemViewModel(
    Guid? profileId,
    string name,
    string environmentLabel,
    string environmentColor)
{
    public Guid? ProfileId { get; } = profileId;

    public string Name { get; } = name;

    public string EnvironmentLabel { get; } = environmentLabel;

    public string EnvironmentColor { get; } = environmentColor;

    public bool IsAllEnvironments => ProfileId is null;

    public bool Matches(DlqSourceItemViewModel source) =>
        IsAllEnvironments || source.ProfileId == ProfileId;
}
