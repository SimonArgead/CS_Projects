using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using System.IO;
using System.Text;
using Npgsql;

#pragma warning disable CS8618, CS8602, CS8604

static class PgHelper
{
    private const string DefaultConn = "Host=localhost;Username=postgres;Password=NOT_GIVING_THAT_HERE_AT_GITHUB;Database=postgres";
    private const string WeatherDbConn = "Host=localhost;Username=postgres;Password=NOT_GIVING_THAT_HERE_AT_GITHUB;Database=weatherdata";

    public static async Task EnsureDatabaseAndTableAsync()
    {
        // Step 1: Ensure database exists
        await using (var conn = new NpgsqlConnection(DefaultConn))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname='weatherdata'", conn);
            var exists = await cmd.ExecuteScalarAsync();
            if (exists == null)
            {
                await using var createCmd = new NpgsqlCommand("CREATE DATABASE weatherdata", conn);
                await createCmd.ExecuteNonQueryAsync();
                Console.WriteLine("Created database 'weatherdata'.");
            }
        }

        // Step 2: Ensure table exists
        await using (var conn = new NpgsqlConnection(WeatherDbConn))
        {
            await conn.OpenAsync();
            string createTable = @"
                CREATE TABLE IF NOT EXISTS observations (
                    station_id   TEXT NOT NULL,
                    observed     TIMESTAMP NOT NULL,
                    parameter_id TEXT NOT NULL,
                    value        DOUBLE PRECISION,
                    PRIMARY KEY (station_id, observed, parameter_id)
                );";
            await using var cmd = new NpgsqlCommand(createTable, conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public static async Task UpsertObservationsAsync(IEnumerable<ObservationPoint> observations)
    {
        await using var conn = new NpgsqlConnection(WeatherDbConn);
        await conn.OpenAsync();

        foreach (var obs in observations)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO observations (station_id, observed, parameter_id, value)
                VALUES (@station, @observed, @param, @value)
                ON CONFLICT (station_id, observed, parameter_id)
                DO UPDATE SET value = EXCLUDED.value;", conn);

            cmd.Parameters.AddWithValue("station", obs.StationId);
            cmd.Parameters.AddWithValue("observed", obs.Observed);
            cmd.Parameters.AddWithValue("param", obs.ParameterId);
            cmd.Parameters.AddWithValue("value", (object?)obs.Value ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();
        }
    }
}

static class Settings
{
    public static string ApiKey = "63ea2db4-d140-4eb2-89b6-ae8b4ebc5663";
    public const string MetObsBase = "https://dmigw.govcloud.dk/v2/metObs";

    public static readonly (double minLon, double minLat, double maxLon, double maxLat) DenmarkBBox =
        (7.0, 54.0, 16.5, 58.0);
}

class StationMeta
{
    public string StationId { get; set; }
    public string Name { get; set; }
    public double Lon { get; set; }
    public double Lat { get; set; }
}

class ObservationPoint
{
    public string StationId { get; set; }
    public DateTime Observed { get; set; }
    public string ParameterId { get; set; }
    public double? Value { get; set; }
}

static class DmiClient
{
    private static readonly HttpClient Http = new HttpClient();

    private static bool WithinBBox(double lon, double lat, (double minLon, double minLat, double maxLon, double maxLat) bbox)
    {
        return lon >= bbox.minLon && lon <= bbox.maxLon && lat >= bbox.minLat && lat <= bbox.maxLat;
    }

    // Step 1: List stations in bounding box
    public static async Task<List<StationMeta>> ListStationsInBBoxAsync()
    {
        var url = $"{Settings.MetObsBase}/collections/station/items?api-key={Settings.ApiKey}&limit=10000";
        var json = await Http.GetStringAsync(url);
        using var doc = JsonDocument.Parse(json);

        var stations = new List<StationMeta>();
        var seen = new HashSet<string>();

        foreach (var feature in doc.RootElement.GetProperty("features").EnumerateArray())
        {
            var props = feature.GetProperty("properties");
            var coords = feature.GetProperty("geometry").GetProperty("coordinates").EnumerateArray().ToArray();
            double lon = coords[0].GetDouble();
            double lat = coords[1].GetDouble();

            if (!WithinBBox(lon, lat, Settings.DenmarkBBox)) continue;

            string name = props.GetProperty("name").GetString() ?? "";
            string stationId = props.GetProperty("stationId").GetString() ?? "";
            if (!seen.Add(stationId)) continue;

            stations.Add(new StationMeta { StationId = stationId, Name = name, Lon = lon, Lat = lat });
        }

        return stations;
    }

    // Step 3: Discover parameters by scanning observations
    public static async Task<List<string>> DiscoverStationPropertiesAsync(string stationId, DateTime fromUtc, DateTime toUtc)
    {
        string dateRange = $"{fromUtc:yyyy-MM-ddTHH:mm:ssZ}/{toUtc:yyyy-MM-ddTHH:mm:ssZ}";
        string url = $"{Settings.MetObsBase}/collections/observation/items?api-key={Settings.ApiKey}&stationId={stationId}&datetime={dateRange}&limit=10000";

        var props = new HashSet<string>();
        while (!string.IsNullOrEmpty(url))
        {
            var json = await Http.GetStringAsync(url);
            using var doc = JsonDocument.Parse(json);

            foreach (var f in doc.RootElement.GetProperty("features").EnumerateArray())
            {
                if (f.TryGetProperty("properties", out var propsElem) &&
                    propsElem.TryGetProperty("parameterId", out var pid))
                {
                    var parameterId = pid.GetString();
                    if (!string.IsNullOrEmpty(parameterId))
                        props.Add(parameterId);
                }
            }

            // pagination
            url = null;
            if (doc.RootElement.TryGetProperty("links", out var links))
            {
                foreach (var link in links.EnumerateArray())
                {
                    if (link.GetProperty("rel").GetString() == "next")
                    {
                        url = link.GetProperty("href").GetString();
                        break;
                    }
                }
            }
        }
        return props.OrderBy(x => x).ToList();
    }

    // Step 5: Fetch actual data for chosen parameters
    public static async IAsyncEnumerable<ObservationPoint> FetchObservationsAsync(
        string stationId, string parameterId, DateTime fromUtc, DateTime toUtc)
    {
        string dateRange = $"{fromUtc:yyyy-MM-ddTHH:mm:ssZ}/{toUtc:yyyy-MM-ddTHH:mm:ssZ}";
        string url = $"{Settings.MetObsBase}/collections/observation/items?api-key={Settings.ApiKey}&stationId={stationId}&parameterId={parameterId}&datetime={dateRange}&limit=10000";

        while (!string.IsNullOrEmpty(url))
        {
            var json = await Http.GetStringAsync(url);
            using var doc = JsonDocument.Parse(json);

            foreach (var f in doc.RootElement.GetProperty("features").EnumerateArray())
            {
                var props = f.GetProperty("properties");
                var observedStr = props.GetProperty("observed").GetString()!;
                var observed = DateTime.Parse(observedStr, null, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

                double? value = null;
                if (props.TryGetProperty("value", out var vProp) && vProp.ValueKind == JsonValueKind.Number)
                    value = vProp.GetDouble();

                yield return new ObservationPoint
                {
                    StationId = stationId,
                    Observed = observed,
                    ParameterId = parameterId,
                    Value = value
                };
            }

            // pagination
            url = null;
            if (doc.RootElement.TryGetProperty("links", out var links))
            {
                foreach (var link in links.EnumerateArray())
                {
                    if (link.GetProperty("rel").GetString() == "next")
                    {
                        url = link.GetProperty("href").GetString();
                        break;
                    }
                }
            }
        }
    }
}

class Program
{
    static async Task Main()
    {
        Console.WriteLine("DMI Weather Data Downloader");

        // Step 1: List stations
        var stations = await DmiClient.ListStationsInBBoxAsync();
        for (int i = 0; i < stations.Count; i++)
            Console.WriteLine($"{i}. {stations[i].StationId} - {stations[i].Name}");

        // Step 2: User picks station
        Console.Write("\nEnter station number: ");
        int choice = int.Parse(Console.ReadLine()!);
        var station = stations[choice];
        Console.WriteLine($"Selected: {station.StationId} - {station.Name}");

        // Step 3: Discover parameters
        var fromUtc = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var toUtc = new DateTime(2024, 12, 31, 23, 0, 0, DateTimeKind.Utc);
        var props = await DmiClient.DiscoverStationPropertiesAsync(station.StationId, fromUtc, toUtc);

        Console.WriteLine("\nAvailable parameters:");
        for (int i = 0; i < props.Count; i++)
            Console.WriteLine($"{i}. {props[i]}");

        // Step 4: User chooses parameters
        Console.Write("\nEnter numbers of parameters to download (comma-separated): ");
        var indices = Console.ReadLine()!.Split(',').Select(s => int.Parse(s.Trim())).ToList();
        var chosen = indices.Select(i => props[i]).ToList();

        // Step 5: Download data
        foreach (var param in chosen)
        {
            Console.WriteLine($"\nFetching {param}...");
            await foreach (var obs in DmiClient.FetchObservationsAsync(station.StationId, param, fromUtc, toUtc))
            {
                Console.WriteLine($"{obs.Observed:u} {obs.ParameterId} = {obs.Value}");
            }
        }

        var allObservations = new List<ObservationPoint>();

        foreach (var param in chosen)
        {
            Console.WriteLine($"\nFetching {param}...");
            await foreach (var obs in DmiClient.FetchObservationsAsync(station.StationId, param, fromUtc, toUtc))
            {
                allObservations.Add(obs);
            }
        }

        // Step 6: Store in PostgreSQL
        await PgHelper.EnsureDatabaseAndTableAsync(); // Ensure DB and table exist

        // Insert/Update observations
        await PgHelper.UpsertObservationsAsync(allObservations);

        Console.WriteLine($"\nSaved {allObservations.Count} rows into PostgreSQL 'weatherdata.observations'");

        Console.WriteLine("\nDone.");
    }
}
