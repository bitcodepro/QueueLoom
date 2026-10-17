namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// The message could not be checked: its body was decoded only up to a limit and the part read does not decide the
/// search. Like a regular expression that ran out of time, this is not a "no": the source is reported as incomplete.
/// </summary>
public sealed class IncompleteBodySearchException : Exception
{
    public IncompleteBodySearchException()
        : base("The decoded body is longer than QueueLoom reads, so the search could not check all of it.")
    {
    }
}
