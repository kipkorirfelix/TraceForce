using Microsoft.Data.Sqlite;
using System.Globalization;

public static class Database
{
    private static readonly string DatabasePath =
        Path.Combine(AppContext.BaseDirectory, "eventtracer.db");

    private static readonly string ConnectionString =
        $"Data Source={DatabasePath};Foreign Keys=True";

    public static void Initialize()
    {
        using var connection = OpenConnection();

        using var command = connection.CreateCommand();

        command.CommandText = """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS RawEvents
        (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            TimestampUtc TEXT NOT NULL,
            MachineId TEXT,
            UserName TEXT,
            Source TEXT,
            Provider TEXT,
            EventId INTEGER,
            Level TEXT,
            EventType TEXT,
            ProcessId INTEGER,
            ParentProcessId INTEGER,
            ProcessName TEXT,
            Application TEXT,
            Message TEXT,
            PayloadJson TEXT,
            IngestedUtc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Activities
        (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            StartUtc TEXT NOT NULL,
            EndUtc TEXT,
            MachineId TEXT,
            UserName TEXT,
            Type TEXT,
            Application TEXT,
            ProcessId INTEGER,
            Confidence TEXT,
            Summary TEXT,
            CreatedUtc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ActivityEventLinks
        (
            ActivityId INTEGER NOT NULL,
            EventId INTEGER NOT NULL,
            Relationship TEXT,
            PRIMARY KEY (ActivityId, EventId),
            FOREIGN KEY (ActivityId)
                REFERENCES Activities(Id)
                ON DELETE CASCADE,
            FOREIGN KEY (EventId)
                REFERENCES RawEvents(Id)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS IX_RawEvents_Timestamp
            ON RawEvents(TimestampUtc);

        CREATE INDEX IF NOT EXISTS IX_RawEvents_ProcessId
            ON RawEvents(ProcessId);

        CREATE INDEX IF NOT EXISTS IX_RawEvents_EventType
            ON RawEvents(EventType);

        CREATE INDEX IF NOT EXISTS IX_RawEvents_Application
            ON RawEvents(Application);

        CREATE INDEX IF NOT EXISTS IX_Activities_Start
            ON Activities(StartUtc);

        CREATE INDEX IF NOT EXISTS IX_Activities_ProcessId
            ON Activities(ProcessId);

        CREATE INDEX IF NOT EXISTS IX_ActivityEventLinks_Event
            ON ActivityEventLinks(EventId);
        """;

        command.ExecuteNonQuery();
    }

    private static SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(ConnectionString);
        connection.Open();
        return connection;
    }

    public static long InsertEvent(RawEvent item)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        INSERT INTO RawEvents
        (
            TimestampUtc,
            MachineId,
            UserName,
            Source,
            Provider,
            EventId,
            Level,
            EventType,
            ProcessId,
            ParentProcessId,
            ProcessName,
            Application,
            Message,
            PayloadJson,
            IngestedUtc
        )
        VALUES
        (
            $timestamp,
            $machine,
            $user,
            $source,
            $provider,
            $eventId,
            $level,
            $eventType,
            $processId,
            $parentProcessId,
            $processName,
            $application,
            $message,
            $payload,
            $ingested
        );

        SELECT last_insert_rowid();
        """;

        command.Parameters.AddWithValue(
            "$timestamp",
            item.TimestampUtc.ToUniversalTime().ToString("O"));

        command.Parameters.AddWithValue(
            "$machine",
            (object?)item.MachineId ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$user",
            (object?)item.UserName ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$source",
            (object?)item.Source ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$provider",
            (object?)item.Provider ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$eventId",
            item.EventId);

        command.Parameters.AddWithValue(
            "$level",
            (object?)item.Level ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$eventType",
            (object?)item.EventType ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$processId",
            (object?)item.ProcessId ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$parentProcessId",
            (object?)item.ParentProcessId ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$processName",
            (object?)item.ProcessName ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$application",
            (object?)item.Application ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$message",
            (object?)item.Message ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$payload",
            (object?)item.PayloadJson ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$ingested",
            DateTime.UtcNow.ToString("O"));

        return Convert.ToInt64(command.ExecuteScalar());
    }

    public static List<RawEvent> GetEvents(
        int limit = 100,
        int offset = 0)
    {
        limit = Math.Clamp(limit, 1, 1000);
        offset = Math.Max(offset, 0);

        var results = new List<RawEvent>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        SELECT
            Id,
            TimestampUtc,
            MachineId,
            UserName,
            Source,
            Provider,
            EventId,
            Level,
            EventType,
            ProcessId,
            ParentProcessId,
            ProcessName,
            Application,
            Message,
            PayloadJson
        FROM RawEvents
        ORDER BY TimestampUtc DESC
        LIMIT $limit OFFSET $offset;
        """;

        command.Parameters.AddWithValue("$limit", limit);
        command.Parameters.AddWithValue("$offset", offset);

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new RawEvent
            {
                Id = reader.GetInt64(0),
                TimestampUtc = ParseDateTime(reader.GetString(1)),
                MachineId = GetString(reader, 2),
                UserName = GetString(reader, 3),
                Source = GetString(reader, 4),
                Provider = GetString(reader, 5),
                EventId = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                Level = GetString(reader, 7),
                EventType = GetString(reader, 8),
                ProcessId = GetNullableInt(reader, 9),
                ParentProcessId = GetNullableInt(reader, 10),
                ProcessName = GetString(reader, 11),
                Application = GetString(reader, 12),
                Message = GetString(reader, 13),
                PayloadJson = GetString(reader, 14)
            });
        }

        return results;
    }

    public static long InsertActivity(Activity activity)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        INSERT INTO Activities
        (
            StartUtc,
            EndUtc,
            MachineId,
            UserName,
            Type,
            Application,
            ProcessId,
            Confidence,
            Summary,
            CreatedUtc
        )
        VALUES
        (
            $start,
            $end,
            $machine,
            $user,
            $type,
            $application,
            $processId,
            $confidence,
            $summary,
            $created
        );

        SELECT last_insert_rowid();
        """;

        command.Parameters.AddWithValue(
            "$start",
            activity.StartUtc.ToUniversalTime().ToString("O"));

        command.Parameters.AddWithValue(
            "$end",
            activity.EndUtc.HasValue
                ? activity.EndUtc.Value.ToUniversalTime().ToString("O")
                : DBNull.Value);

        command.Parameters.AddWithValue(
            "$machine",
            (object?)activity.MachineId ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$user",
            (object?)activity.UserName ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$type",
            (object?)activity.Type ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$application",
            (object?)activity.Application ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$processId",
            (object?)activity.ProcessId ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$confidence",
            (object?)activity.Confidence ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$summary",
            (object?)activity.Summary ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$created",
            activity.CreatedUtc.ToUniversalTime().ToString("O"));

        return Convert.ToInt64(command.ExecuteScalar());
    }

    public static void UpdateActivityEnd(
        long activityId,
        DateTime endUtc,
        string confidence,
        string summary)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        UPDATE Activities
        SET
            EndUtc = $end,
            Confidence = $confidence,
            Summary = $summary
        WHERE Id = $id;
        """;

        command.Parameters.AddWithValue(
            "$end",
            endUtc.ToUniversalTime().ToString("O"));

        command.Parameters.AddWithValue(
            "$confidence",
            (object?)confidence ?? DBNull.Value);

        command.Parameters.AddWithValue(
            "$summary",
            (object?)summary ?? DBNull.Value);

        command.Parameters.AddWithValue("$id", activityId);

        command.ExecuteNonQuery();
    }

    public static void LinkEvent(
        long activityId,
        long eventId,
        string relationship)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        INSERT OR IGNORE INTO ActivityEventLinks
        (
            ActivityId,
            EventId,
            Relationship
        )
        VALUES
        (
            $activityId,
            $eventId,
            $relationship
        );
        """;

        command.Parameters.AddWithValue("$activityId", activityId);
        command.Parameters.AddWithValue("$eventId", eventId);
        command.Parameters.AddWithValue(
            "$relationship",
            (object?)relationship ?? DBNull.Value);

        command.ExecuteNonQuery();
    }

    public static List<Activity> GetActivities(int limit = 100)
    {
        limit = Math.Clamp(limit, 1, 1000);

        var results = new List<Activity>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
        SELECT
            Id,
            StartUtc,
            EndUtc,
            MachineId,
            UserName,
            Type,
            Application,
            ProcessId,
            Confidence,
            Summary,
            CreatedUtc
        FROM Activities
        ORDER BY StartUtc DESC
        LIMIT $limit;
        """;

        command.Parameters.AddWithValue("$limit", limit);

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new Activity
            {
                Id = reader.GetInt64(0),
                StartUtc = ParseDateTime(reader.GetString(1)),
                EndUtc = reader.IsDBNull(2)
                    ? null
                    : ParseDateTime(reader.GetString(2)),
                MachineId = GetString(reader, 3),
                UserName = GetString(reader, 4),
                Type = GetString(reader, 5),
                Application = GetString(reader, 6),
                ProcessId = GetNullableInt(reader, 7),
                Confidence = GetString(reader, 8),
                Summary = GetString(reader, 9),
                CreatedUtc = ParseDateTime(reader.GetString(10))
            });
        }

        return results;
    }

    public static Statistics GetStatistics()
    {
        using var connection = OpenConnection();

        long totalEvents;
        long totalActivities;
        long totalProcesses;
        long eventsLastHour;

        using (var command = connection.CreateCommand())
        {
            command.CommandText =
                "SELECT COUNT(*) FROM RawEvents;";

            totalEvents = Convert.ToInt64(
                command.ExecuteScalar());
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText =
                "SELECT COUNT(*) FROM Activities;";

            totalActivities = Convert.ToInt64(
                command.ExecuteScalar());
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
            SELECT COUNT(DISTINCT ProcessId)
            FROM RawEvents
            WHERE ProcessId IS NOT NULL;
            """;

            totalProcesses = Convert.ToInt64(
                command.ExecuteScalar());
        }

        using (var command = connection.CreateCommand())
        {
            var cutoff = DateTime.UtcNow.AddHours(-1);

            command.CommandText = """
            SELECT COUNT(*)
            FROM RawEvents
            WHERE TimestampUtc >= $cutoff;
            """;

            command.Parameters.AddWithValue(
                "$cutoff",
                cutoff.ToString("O"));

            eventsLastHour = Convert.ToInt64(
                command.ExecuteScalar());
        }

        return new Statistics
        {
            TotalEvents = totalEvents,
            TotalActivities = totalActivities,
            TotalProcesses = totalProcesses,
            EventsLastHour = eventsLastHour
        };
    }

    public static void Cleanup(int days)
    {
        days = Math.Clamp(days, 1, 3650);

        var cutoff = DateTime.UtcNow.AddDays(-days)
            .ToString("O");

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;

                command.CommandText = """
                DELETE FROM ActivityEventLinks
                WHERE EventId IN
                (
                    SELECT Id
                    FROM RawEvents
                    WHERE TimestampUtc < $cutoff
                );
                """;

                command.Parameters.AddWithValue(
                    "$cutoff",
                    cutoff);

                command.ExecuteNonQuery();
            }

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;

                command.CommandText = """
                DELETE FROM RawEvents
                WHERE TimestampUtc < $cutoff;
                """;

                command.Parameters.AddWithValue(
                    "$cutoff",
                    cutoff);

                command.ExecuteNonQuery();
            }

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;

                command.CommandText = """
                DELETE FROM Activities
                WHERE StartUtc < $cutoff;
                """;

                command.Parameters.AddWithValue(
                    "$cutoff",
                    cutoff);

                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    private static string? GetString(
        SqliteDataReader reader,
        int index)
    {
        return reader.IsDBNull(index)
            ? null
            : reader.GetString(index);
    }

    private static int? GetNullableInt(
        SqliteDataReader reader,
        int index)
    {
        return reader.IsDBNull(index)
            ? null
            : reader.GetInt32(index);
    }

    private static DateTime ParseDateTime(string value)
    {
        return DateTime.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);
    }
}

public sealed class Statistics
{
    public long TotalEvents { get; set; }
    public long TotalActivities { get; set; }
    public long TotalProcesses { get; set; }
    public long EventsLastHour { get; set; }
}


