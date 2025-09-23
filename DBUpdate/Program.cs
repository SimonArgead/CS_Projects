using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;

#pragma warning disable CS8618, CS8602, CS8604

// ============================
// Domain models
// ============================

class StationMeta
{
    public string StationId { get; set; }
    public string Name { get; set; }
    public double Lon { get; set; }
    public double Lat { get; set; }
    public List<string> Parameters { get; set; } = new();
}

class ObservationPoint
{
    public string StationId { get; set; }
    public DateTime Observed { get; set; }
    public double? Value { get; set; }
    public string ParameterId { get; set; }
}

class ObservationRow
{
    public string StationId { get; set; } = "";
    public DateTime ObservationTime { get; set; } // UTC

    public double? Rain { get; set; }
    public double? RainMinutes { get; set; }
    public double? AverageTemperature { get; set; }
    public double? MaximumTemperature { get; set; }
    public double? MinimumTemperature { get; set; }
    public double? WindSpeed { get; set; }
    public double? MaximumWindSpeed { get; set; }
    public double? WindDir { get; set; } // Added to align with ParameterMap
    public double? SunMinutes { get; set; }
    public double? Cloud { get; set; }
    public double? Humidity { get; set; }
    public double? Pressure { get; set; }
}

// ============================
// Settings
// ============================

static class Settings
{
    public static string ApiKey = "63ea2db4-d140-4eb2-89b6-ae8b4ebc5663";
    public const string MetObsBase = "https://dmigw.govcloud.dk/v2/metObs";
    public static int Year = 2024;

    // Friendly label -> actual DMI parameter ID
    public static readonly Dictionary<string, string> ParameterMap = new()
    { // label to parameterId. What we call it in the dataset/what it's called in DMI MetObs
        { "rain", "precip_past10min" },
        { "rain_minutes", "precip_dur_past10min" },
        { "average_temperature", "temp_mean_past10min" },
        { "maximum_temperature", "temp_max_past10min" },
        { "minimum_temperature", "temp_min_past10min" },
        { "wind_speed", "wind_speed" },
        { "maximum_wind_speed", "wind_max" },
        { "wind_dir", "wind_dir" },
        { "sun_minutes", "sun_last10min_glob" },
        { "cloud", "cloud_cover" },
        { "humidity", "humidity" },
        { "pressure", "pressure" }
    };

    public static readonly (double minLon, double minLat, double maxLon, double maxLat) DenmarkBBox =
        (7.0, 54.0, 16.5, 58.0);

    public static string PgHost = "localhost";
    public static int PgPort = 5432;
    public static string PgUser = "postgres";
    public static string PgPassword = "1314";
    public static string PgDatabase = "weatherdata";

    public static string ServerConn =>
        $"Host={PgHost};Port={PgPort};Username={PgUser};Password={PgPassword};Database=postgres";
    public static string DbConn =>
        $"Host={PgHost};Port={PgPort};Username={PgUser};Password={PgPassword};Database={PgDatabase}";
}

// ============================
// DMI client
// ============================

static class DmiClient
{
    private static readonly HttpClient Http = new HttpClient();

    private static bool WithinBBox(double lon, double lat, (double minLon, double minLat, double maxLon, double maxLat) bbox)
    {
        return lon >= bbox.minLon && lon <= bbox.maxLon && lat >= bbox.minLat && lat <= bbox.maxLat;
    }

    public static async Task ListStationsInBBoxAsync()
    {
        var url = $"{Settings.MetObsBase}/collections/station/items?api-key={Settings.ApiKey}&limit=10000";
        var json = await Http.GetStringAsync(url);
        using var doc = JsonDocument.Parse(json);

        var seenStations = new HashSet<string>();

        Console.WriteLine("=== Stations inside bounding box ===");
        foreach (var feature in doc.RootElement.GetProperty("features").EnumerateArray())
        {
            var props = feature.GetProperty("properties");
            var coords = feature.GetProperty("geometry").GetProperty("coordinates").EnumerateArray().ToArray();
            double lon = coords[0].GetDouble();
            double lat = coords[1].GetDouble();

            if (!WithinBBox(lon, lat, Settings.DenmarkBBox)) continue;

            string name = props.GetProperty("name").GetString() ?? "";
            string stationId = props.GetProperty("stationId").GetString() ?? "";
            if (!seenStations.Add(stationId)) continue;

            // Exclude obvious offshore names
            if (name.Contains("Sea", StringComparison.OrdinalIgnoreCase) ||
                name.Contains("Hav", StringComparison.OrdinalIgnoreCase) ||
                name.Contains("Offshore", StringComparison.OrdinalIgnoreCase))
                continue;

            Console.WriteLine($"{stationId} - {name} | lon={lon:F4}, lat={lat:F4}");
        }
    }

    public static async Task<List<string>> GetObservedPropertiesForStationAsync(string stationId, int limit = 500)
    {
        var url = $"{Settings.MetObsBase}/collections/observation/items?api-key={Settings.ApiKey}&stationId={stationId}&limit={limit}";
        var json = await Http.GetStringAsync(url);
        using var doc = JsonDocument.Parse(json);

        var props = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (doc.RootElement.TryGetProperty("features", out var feats))
        {
            foreach (var f in feats.EnumerateArray())
            {
                var p = f.GetProperty("properties");
                var observedProperty = p.TryGetProperty("observedProperty", out var op) ? op.GetString() : null;
                if (!string.IsNullOrWhiteSpace(observedProperty))
                    props.Add(observedProperty!);
            }
        }

        return props.OrderBy(x => x).ToList();
    }

    public static async Task ShowStationParametersAsync(string stationId)
    {
        var rawParams = await GetObservedPropertiesForStationAsync(stationId);

        var have = new List<string>();
        var missing = new List<string>();

        foreach (var kvp in Settings.ParameterMap)
        {
            string label = kvp.Key;
            string dmiId = kvp.Value;

            if (rawParams.Contains(dmiId))
                have.Add($"{label} ({dmiId})");
            else
                missing.Add($"{label} ({dmiId})");
        }

        Console.WriteLine($"\n=== Parameters for station {stationId} ===");
        Console.WriteLine($"Raw observed properties: {string.Join(", ", rawParams)}");
        Console.WriteLine($"Have (from desired list): {string.Join(", ", have)}");
        Console.WriteLine($"Missing (from desired list): {string.Join(", ", missing)}");
    }

    public static async Task<Dictionary<string, string>> ChooseParameterIdsForStationAsync(string stationId)
    {
        var raw = await GetObservedPropertiesForStationAsync(stationId);
        var chosen = new Dictionary<string, string>();

        foreach (var kvp in Settings.ParameterMap)
        {
            string label = kvp.Key;
            string preferred = kvp.Value;

            if (raw.Contains(preferred))
            {
                chosen[label] = preferred;
                continue;
            }

            // Simple fallback between 10-min and hourly forms
            var fallbacks = new List<string>();
            if (preferred.Contains("past10min", StringComparison.OrdinalIgnoreCase))
                fallbacks.Add(preferred.Replace("past10min", "past1h"));
            else if (preferred.Contains("past1h", StringComparison.OrdinalIgnoreCase))
                fallbacks.Add(preferred.Replace("past1h", "past10min"));

            var match = fallbacks.FirstOrDefault(id => raw.Contains(id));
            if (match != null)
                chosen[label] = match;
        }

        return chosen;
    }

    public static async IAsyncEnumerable<ObservationPoint> FetchObservationsHourlyAsync(
        string parameterId, DateTime fromUtc, DateTime toUtc, HashSet<string> allowedStationIds)
    {
        string dateRange = $"{fromUtc:yyyy-MM-ddTHH:mm:ssZ}/{toUtc:yyyy-MM-ddTHH:mm:ssZ}";
        string url = $"{Settings.MetObsBase}/collections/observation/items" +
                     $"?api-key={Settings.ApiKey}&datetime={dateRange}&parameterId={parameterId}&limit=10000";

        while (!string.IsNullOrEmpty(url))
        {
            var json = await Http.GetStringAsync(url);
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("features", out var feats))
            {
                foreach (var feature in feats.EnumerateArray())
                {
                    var props = feature.GetProperty("properties");
                    string stationId = props.GetProperty("stationId").GetString()!;
                    if (!allowedStationIds.Contains(stationId))
                        continue;

                    var observedStr = props.GetProperty("observed").GetString()!;
                    var observed = DateTime.Parse(observedStr, null, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
                    observed = new DateTime(observed.Year, observed.Month, observed.Day, observed.Hour, 0, 0, DateTimeKind.Utc);

                    double? value = null;
                    if (props.TryGetProperty("value", out var vProp) && vProp.ValueKind == JsonValueKind.Number)
                        value = vProp.GetDouble();

                    yield return new ObservationPoint
                    {
                        StationId = stationId,
                        Observed = observed,
                        Value = value,
                        ParameterId = parameterId
                    };
                }
            }

            url = null;
            if (doc.RootElement.TryGetProperty("links", out var links))
            {
                foreach (var link in links.EnumerateArray())
                {
                    var rel = link.TryGetProperty("rel", out var r) ? r.GetString() : null;
                    if (string.Equals(rel, "next", StringComparison.OrdinalIgnoreCase))
                    {
                        url = link.GetProperty("href").GetString();
                        break;
                    }
                }
            }
        }
    }
}

// ============================
// Database helpers
// ============================

static class Pg
{
    public static void EnsureDatabase()
    {
        using var conn = new NpgsqlConnection(Settings.ServerConn);
        conn.Open();
        using var checkCmd = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @name", conn);
        checkCmd.Parameters.AddWithValue("name", Settings.PgDatabase);
        var exists = checkCmd.ExecuteScalar();
        if (exists == null)
        {
            using var createCmd = new NpgsqlCommand($"CREATE DATABASE \"{Settings.PgDatabase}\"", conn);
            createCmd.ExecuteNonQuery();
            Console.WriteLine($"Created database {Settings.PgDatabase}.");
        }
        else
        {
            Console.WriteLine($"Database {Settings.PgDatabase} already exists.");
        }
    }

    public static void EnsureTable()
    {
        using var conn = new NpgsqlConnection(Settings.DbConn);
        conn.Open();
        var sql = @"
        CREATE TABLE IF NOT EXISTS weather_observations (
            id SERIAL PRIMARY KEY,
            station_id TEXT NOT NULL,
            observation_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            rain DOUBLE PRECISION,
            rain_minutes DOUBLE PRECISION,
            average_temperature DOUBLE PRECISION,
            maximum_temperature DOUBLE PRECISION,
            minimum_temperature DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            maximum_wind_speed DOUBLE PRECISION,
            wind_dir DOUBLE PRECISION,
            sun_minutes DOUBLE PRECISION,
            cloud DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            CONSTRAINT uq_station_time UNIQUE (station_id, observation_time)
        );
        CREATE INDEX IF NOT EXISTS idx_obs_station_time ON weather_observations (station_id, observation_time);
        ";
        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.ExecuteNonQuery();
    }

    public static async Task UpsertBatchAsync(IEnumerable<ObservationRow> rows)
    {
        using var conn = new NpgsqlConnection(Settings.DbConn);
        await conn.OpenAsync();
        using var tx = await conn.BeginTransactionAsync();

        var sql = @"
        INSERT INTO weather_observations
        (station_id, observation_time, rain, rain_minutes, average_temperature, maximum_temperature, minimum_temperature,
         wind_speed, maximum_wind_speed, wind_dir, sun_minutes, cloud, humidity, pressure)
        VALUES
        (@station_id, @observation_time, @rain, @rain_minutes, @average_temperature, @maximum_temperature, @minimum_temperature,
         @wind_speed, @maximum_wind_speed, @wind_dir, @sun_minutes, @cloud, @humidity, @pressure)
        ON CONFLICT (station_id, observation_time) DO UPDATE SET
            rain = COALESCE(EXCLUDED.rain, weather_observations.rain),
            rain_minutes = COALESCE(EXCLUDED.rain_minutes, weather_observations.rain_minutes),
            average_temperature = COALESCE(EXCLUDED.average_temperature, weather_observations.average_temperature),
            maximum_temperature = COALESCE(EXCLUDED.maximum_temperature, weather_observations.maximum_temperature),
            minimum_temperature = COALESCE(EXCLUDED.minimum_temperature, weather_observations.minimum_temperature),
            wind_speed = COALESCE(EXCLUDED.wind_speed, weather_observations.wind_speed),
            maximum_wind_speed = COALESCE(EXCLUDED.maximum_wind_speed, weather_observations.maximum_wind_speed),
            wind_dir = COALESCE(EXCLUDED.wind_dir, weather_observations.wind_dir),
            sun_minutes = COALESCE(EXCLUDED.sun_minutes, weather_observations.sun_minutes),
            cloud = COALESCE(EXCLUDED.cloud, weather_observations.cloud),
            humidity = COALESCE(EXCLUDED.humidity, weather_observations.humidity),
            pressure = COALESCE(EXCLUDED.pressure, weather_observations.pressure);
        ";

        foreach (var row in rows)
        {
            using var cmd = new NpgsqlCommand(sql, conn, (NpgsqlTransaction)tx);
            cmd.Parameters.AddWithValue("station_id", row.StationId);
            cmd.Parameters.AddWithValue("observation_time", row.ObservationTime);
            cmd.Parameters.AddWithValue("rain", (object?)row.Rain ?? DBNull.Value);
            cmd.Parameters.AddWithValue("rain_minutes", (object?)row.RainMinutes ?? DBNull.Value);
            cmd.Parameters.AddWithValue("average_temperature", (object?)row.AverageTemperature ?? DBNull.Value);
            cmd.Parameters.AddWithValue("maximum_temperature", (object?)row.MaximumTemperature ?? DBNull.Value);
            cmd.Parameters.AddWithValue("minimum_temperature", (object?)row.MinimumTemperature ?? DBNull.Value);
            cmd.Parameters.AddWithValue("wind_speed", (object?)row.WindSpeed ?? DBNull.Value);
            cmd.Parameters.AddWithValue("maximum_wind_speed", (object?)row.MaximumWindSpeed ?? DBNull.Value);
            cmd.Parameters.AddWithValue("wind_dir", (object?)row.WindDir ?? DBNull.Value);
            cmd.Parameters.AddWithValue("sun_minutes", (object?)row.SunMinutes ?? DBNull.Value);
            cmd.Parameters.AddWithValue("cloud", (object?)row.Cloud ?? DBNull.Value);
            cmd.Parameters.AddWithValue("humidity", (object?)row.Humidity ?? DBNull.Value);
            cmd.Parameters.AddWithValue("pressure", (object?)row.Pressure ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }
}

// ============================
// Orchestration
// ============================

class Program
{
    static async Task Main()
    {
        Console.WriteLine("DMI Weather -> PostgreSQL (hourly, per-station, truthful parameter inspection)");
        Console.WriteLine($"Year: {Settings.Year}");

        // 1) Ensure database and table
        Pg.EnsureDatabase();
        Pg.EnsureTable();

        // 2) List stations inside bbox and let user pick one by ID
        await DmiClient.ListStationsInBBoxAsync();
        Console.Write("\nEnter station ID to inspect and ingest: ");
        string stationId = Console.ReadLine()?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(stationId))
        {
            Console.WriteLine("No station ID entered. Exiting.");
            return;
        }

        // 3) Show parameters for that station (raw + have/missing vs desired list)
        await DmiClient.ShowStationParametersAsync(stationId);

        // 4) Choose parameter IDs for ingestion based on availability (with simple 10-min/hourly fallback)
        var chosenIds = await DmiClient.ChooseParameterIdsForStationAsync(stationId);
        if (chosenIds.Count == 0)
        {
            Console.WriteLine("No desired parameters available for this station. Exiting.");
            return;
        }

        Console.WriteLine("\nChosen parameter IDs for ingestion:");
        foreach (var kv in chosenIds)
            Console.WriteLine($"- {kv.Key}: {kv.Value}");

        // 5) Build full hourly timeline
        var fromUtc = new DateTime(Settings.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var toUtc = new DateTime(Settings.Year, 12, 31, 23, 0, 0, DateTimeKind.Utc);
        var fullTimeline = GenerateHourlyTimeline(fromUtc, toUtc);

        // 6) Prepare master structure for just the selected station
        var allowedStationIds = new HashSet<string> { stationId };
        var master = new Dictionary<string, Dictionary<DateTime, ObservationRow>>
        {
            [stationId] = fullTimeline.ToDictionary(
                ts => ts,
                ts => new ObservationRow { StationId = stationId, ObservationTime = ts }
            )
        };

        // 7) Fetch observations for parameters the station actually has
        Console.WriteLine("\nFetching observations...");
        foreach (var (label, parameterId) in chosenIds)
        {
            Console.WriteLine($" - {label} ({parameterId})");
            await foreach (var obs in DmiClient.FetchObservationsHourlyAsync(parameterId, fromUtc, toUtc, allowedStationIds))
            {
                var row = master[obs.StationId][obs.Observed];
                switch (label)
                {
                    case "rain": row.Rain = obs.Value; break;
                    case "rain_minutes": row.RainMinutes = obs.Value; break;
                    case "average_temperature": row.AverageTemperature = obs.Value; break;
                    case "maximum_temperature": row.MaximumTemperature = obs.Value; break;
                    case "minimum_temperature": row.MinimumTemperature = obs.Value; break;
                    case "wind_speed": row.WindSpeed = obs.Value; break;
                    case "maximum_wind_speed": row.MaximumWindSpeed = obs.Value; break;
                    case "wind_dir": row.WindDir = obs.Value; break;
                    case "sun_minutes": row.SunMinutes = obs.Value; break;
                    case "cloud": row.Cloud = obs.Value; break;
                    case "humidity": row.Humidity = obs.Value; break;
                    case "pressure": row.Pressure = obs.Value; break;
                }
            }
        }

        // 8) Save to PostgreSQL in batches
        Console.WriteLine("\nSaving to PostgreSQL...");
        var rows = master[stationId].Values.OrderBy(r => r.ObservationTime).ToList();
        const int batchSize = 5000;
        for (int i = 0; i < rows.Count; i += batchSize)
        {
            var batch = rows.Skip(i).Take(batchSize);
            await Pg.UpsertBatchAsync(batch);
            Console.WriteLine($"  Inserted {Math.Min(i + batchSize, rows.Count)}/{rows.Count} rows");
        }

        Console.WriteLine("Done.");
    }

    static List<DateTime> GenerateHourlyTimeline(DateTime fromUtc, DateTime toUtc)
    {
        var list = new List<DateTime>();
        for (var t = fromUtc; t <= toUtc; t = t.AddHours(1))
            list.Add(t);
        return list;
    }
}
