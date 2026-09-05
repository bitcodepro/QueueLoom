using System.Data.Common;

namespace QueueLoom.Infrastructure.Azure;

public static class EmulatorConnection
{
    public static bool IsEmulator(string connectionString)
    {
        var values = new DbConnectionStringBuilder { ConnectionString = connectionString };
        return values.TryGetValue("UseDevelopmentEmulator", out var value) &&
               bool.TryParse(Convert.ToString(value), out var enabled) && enabled;
    }

    public static string AdministrationConnectionString(string connectionString, int managementPort = 5300)
    {
        if (!IsEmulator(connectionString)) return connectionString;
        ArgumentOutOfRangeException.ThrowIfLessThan(managementPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(managementPort, 65535);
        var values = new DbConnectionStringBuilder { ConnectionString = connectionString };
        var endpoint = new UriBuilder(Convert.ToString(values["Endpoint"])!) { Port = managementPort };
        values["Endpoint"] = endpoint.Uri.AbsoluteUri;
        return values.ConnectionString;
    }
}
