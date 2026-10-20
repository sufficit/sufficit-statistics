using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Sufficit.Statistics
{
    internal sealed class LocalMetricsQueue : IDisposable
    {
        private readonly object _gate = new object();
        private readonly FileStream _lease;
        private readonly SqliteConnection _connection;
        private readonly string _path;
        private readonly int _maxMetricBytes;
        private readonly JsonSerializerOptions _json = new JsonSerializerOptions();
        private bool _disposed;

        public LocalMetricsQueue(MetricsQueueOptions options)
        {
            options.Validate();
            _maxMetricBytes = options.MaxMetricBytes;
            Directory.CreateDirectory(options.DirectoryPath);
            _path = Path.Combine(options.DirectoryPath, "pending.sqlite");
            _lease = new FileStream(Path.Combine(options.DirectoryPath, "owner.lock"), FileMode.OpenOrCreate,
                FileAccess.ReadWrite, FileShare.None);
            _connection = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = _path, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false
            }.ToString());
            try
            {
                _connection.Open();
                _json.Converters.Add(new MetricFieldJsonConverter());
                if (Convert.ToInt64(Scalar("PRAGMA user_version;")) > 1)
                    throw new InvalidDataException("Metrics queue schema is newer than this runtime.");
                Execute("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;");
                var pageSize = Convert.ToInt64(Scalar("PRAGMA page_size;"));
                var pageCount = Convert.ToInt64(Scalar("PRAGMA page_count;"));
                if (pageCount > options.MaxDatabaseBytes / pageSize)
                    throw new IOException("Existing metrics queue exceeds configured database capacity.");
                Execute("PRAGMA max_page_count=" + (options.MaxDatabaseBytes / pageSize) + ";");
                Execute("CREATE TABLE IF NOT EXISTS pending (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created INTEGER NOT NULL);" +
                    "CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), delivered INTEGER); PRAGMA user_version=1;");
            }
            catch { _connection.Dispose(); _lease.Dispose(); throw; }
        }

        public void Enqueue(Metric metric) => EnqueueBatch(new[] { metric });

        public void EnqueueBatch(IReadOnlyCollection<Metric> metrics)
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                using var transaction = _connection.BeginTransaction();
                using var command = _connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = "INSERT INTO pending(payload,created) VALUES ($payload,$created);";
                var payloadParameter = command.Parameters.Add("$payload", SqliteType.Text);
                var createdParameter = command.Parameters.Add("$created", SqliteType.Integer);
                foreach (var metric in metrics)
                {
                    if (metric == null || string.IsNullOrWhiteSpace(metric.Measurement) || metric.Fields == null || metric.Fields.Count == 0)
                        throw new ArgumentException("A metric requires measurement and fields.", nameof(metrics));
                    var snapshot = new Metric
                    {
                        Measurement = metric.Measurement, Fields = metric.Fields, Tags = metric.Tags,
                        Timestamp = metric.Timestamp == default ? DateTime.UtcNow : metric.Timestamp.ToUniversalTime()
                    };
                    var payload = JsonSerializer.Serialize(snapshot, _json);
                    if (Encoding.UTF8.GetByteCount(payload) > _maxMetricBytes)
                        throw new ArgumentException("Metric exceeds queue item limit.", nameof(metrics));
                    payloadParameter.Value = payload;
                    createdParameter.Value = DateTime.UtcNow.Ticks;
                    command.ExecuteNonQuery();
                }
                transaction.Commit(); // Entire bounded batch accepted locally, or none of it.
            }
        }

        public List<(long Id, Metric Metric)> ReadBatch(int count)
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                var batch = new List<(long, Metric)>();
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT id,payload FROM pending ORDER BY id LIMIT $count;";
                command.Parameters.AddWithValue("$count", count);
                using var reader = command.ExecuteReader();
                while (reader.Read())
                    batch.Add((reader.GetInt64(0), JsonSerializer.Deserialize<Metric>(reader.GetString(1), _json)
                        ?? throw new InvalidDataException("Invalid persisted metric.")));
                return batch;
            }
        }

        public void Acknowledge(long lastId)
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                using var transaction = _connection.BeginTransaction();
                using var command = _connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = "DELETE FROM pending WHERE id <= $id; INSERT OR REPLACE INTO state(id,delivered) VALUES(1,$now);";
                command.Parameters.AddWithValue("$id", lastId);
                command.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                command.ExecuteNonQuery();
                transaction.Commit();
            }
        }

        public MetricsQueueStatus Status()
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                return new MetricsQueueStatus
                {
                    PendingCount = Convert.ToInt64(Scalar("SELECT COUNT(*) FROM pending;")),
                    DatabaseBytes = new FileInfo(_path).Length,
                    OldestPendingUtc = Date(Scalar("SELECT MIN(created) FROM pending;")),
                    LastDeliveryUtc = Date(Scalar("SELECT delivered FROM state WHERE id=1;"))
                };
            }
        }
        private static DateTime? Date(object? value) => value == null || value == DBNull.Value
            ? (DateTime?)null : new DateTime(Convert.ToInt64(value), DateTimeKind.Utc);
        private object? Scalar(string sql) { using var c = _connection.CreateCommand(); c.CommandText = sql; return c.ExecuteScalar(); }
        private void Execute(string sql) { using var c = _connection.CreateCommand(); c.CommandText = sql; c.ExecuteNonQuery(); }
        private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(LocalMetricsQueue)); }
        public void Dispose() { lock (_gate) { if (_disposed) return; _disposed = true; _connection.Dispose(); _lease.Dispose(); } }
    }
}
