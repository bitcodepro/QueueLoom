using System.Text;
using System.Text.Json;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// Dead-letter history in one JSON Lines file, loaded once and appended to. Samples older than
/// <see cref="DeadLetterHistory.Retention"/> are dropped when the file is rewritten, at most once a day.
/// </summary>
public sealed class JsonLinesDeadLetterHistoryStore(string file, TimeProvider? time = null) : IDeadLetterHistoryStore
{
    private readonly object _gate = new();
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private List<DeadLetterHistorySample>? _samples;
    private DateTimeOffset _lastCompaction;

    public void Append(DeadLetterHistorySample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        lock (_gate)
        {
            var samples = Load();
            var previous = samples.LastOrDefault(item => item.ProfileId == sample.ProfileId);
            if (previous is not null && sample.At - previous.At < DeadLetterHistory.MinimumSpacing && previous.Total == sample.Total)
            {
                return;
            }

            samples.Add(sample);
            var now = _time.GetUtcNow();
            if (now - _lastCompaction > TimeSpan.FromDays(1) && samples[0].At < now - DeadLetterHistory.Retention)
            {
                samples.RemoveAll(item => item.At < now - DeadLetterHistory.Retention);
                Rewrite(samples);
                _lastCompaction = now;
                return;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            File.AppendAllText(file, JsonSerializer.Serialize(sample) + "\n", Encoding.UTF8);
        }
    }

    public IReadOnlyList<DeadLetterHistorySample> Read(Guid profileId, DateTimeOffset since)
    {
        lock (_gate)
        {
            return Load().Where(sample => sample.ProfileId == profileId && sample.At >= since).ToArray();
        }
    }

    private List<DeadLetterHistorySample> Load()
    {
        if (_samples is not null)
        {
            return _samples;
        }

        _samples = [];
        if (File.Exists(file))
        {
            foreach (var line in File.ReadLines(file))
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }
                try
                {
                    if (JsonSerializer.Deserialize<DeadLetterHistorySample>(line) is { Sources: not null } sample)
                    {
                        _samples.Add(sample);
                    }
                }
                catch (JsonException)
                {
                    // A line cut short by a crash must not hide the rest of the history.
                }
            }
            _samples.Sort((left, right) => left.At.CompareTo(right.At));
        }
        return _samples;
    }

    private void Rewrite(IReadOnlyList<DeadLetterHistorySample> samples)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        var temporary = file + ".tmp";
        using (var writer = new StreamWriter(temporary, append: false, new UTF8Encoding(false)))
        {
            foreach (var sample in samples)
            {
                writer.Write(JsonSerializer.Serialize(sample));
                writer.Write('\n');
            }
        }
        File.Move(temporary, file, overwrite: true);
    }
}
