using System.Globalization;

namespace QueueLoom.Tests;

// CurrentCulture flows with the test's async context; do not change process-wide defaults.
internal sealed class TestCulture : IDisposable
{
    private readonly CultureInfo _previous = CultureInfo.CurrentCulture;

    public TestCulture(string name) => CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);

    public void Dispose() => CultureInfo.CurrentCulture = _previous;
}
