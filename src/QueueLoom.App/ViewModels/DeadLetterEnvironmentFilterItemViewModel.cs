using QueueLoom.App.Models;
namespace QueueLoom.App.ViewModels;

public sealed class DeadLetterEnvironmentFilterItemViewModel(
    Guid? profileId,
    string name,
    string environmentLabel,
    Tone environmentTone)
{
    public Guid? ProfileId { get; } = profileId;

    public string Name { get; } = name;

    public string EnvironmentLabel { get; } = environmentLabel;

    public Tone EnvironmentTone { get; } = environmentTone;

    public bool IsAllEnvironments => ProfileId is null;

    public bool Matches(DlqSourceItemViewModel source) =>
        ProfileId is not null && source.ProfileId == ProfileId;
}
