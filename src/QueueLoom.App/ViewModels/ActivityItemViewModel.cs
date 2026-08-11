namespace QueueLoom.App.ViewModels;

public sealed record ActivityItemViewModel(
    DateTimeOffset Timestamp,
    string Level,
    string Action,
    string Details)
{
    public string Time => Timestamp.ToLocalTime().ToString("HH:mm:ss");

    public string LevelColor => Level switch
    {
        "Error" => "#FF6B82",
        "Warning" => "#FFB45E",
        "Success" => "#4ADE9D",
        _ => "#91A5BD"
    };
}
