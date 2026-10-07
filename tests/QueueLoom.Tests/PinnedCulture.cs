using System.Globalization;
using System.Runtime.CompilerServices;

namespace QueueLoom.Tests;

/// <summary>
/// The culture every test starts in, set once when the test assembly loads (before any test runs), so results do not
/// depend on the machine. It is deliberately not English: German writes 1.000,5 where English writes 1,000.5, so a
/// test or product path that silently assumes English number or date formatting fails everywhere, including CI,
/// instead of only on a contributor's machine. A test that needs another culture still scopes it with
/// <see cref="TestCulture"/>, which changes only that test's async context.
/// </summary>
internal static class PinnedCulture
{
    public static readonly CultureInfo Culture = CultureInfo.GetCultureInfo("de-DE");

    [ModuleInitializer]
    internal static void Pin()
    {
        CultureInfo.DefaultThreadCurrentCulture = Culture;
        CultureInfo.DefaultThreadCurrentUICulture = Culture;
        CultureInfo.CurrentCulture = Culture;
        CultureInfo.CurrentUICulture = Culture;
    }
}
