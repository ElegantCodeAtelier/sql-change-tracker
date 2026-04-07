using System.Text.Json;
using SqlChangeTracker.Config;
using System.Linq;

namespace SqlChangeTracker.Output;

internal sealed class OutputFormatter
{
    private readonly bool _json;

    public OutputFormatter(bool json)
    {
        _json = json;
    }

    public void WriteConfig(ConfigResult result)
    {
        if (_json)
        {
            var payload = new
            {
                result.Command,
                result.ProjectDir,
                result.Valid,
                result.Errors,
                result.ConfigPath,
                result.Config
            };
            WriteJson(payload);
            return;
        }

        WriteConfigShow(new ConfigShowResult(
            result.Command,
            result.ProjectDir,
            result.ConfigPath,
            result.Config,
            result.Compatibility));

        if (result.Valid)
        {
            Console.WriteLine("Config validation: ok");
            return;
        }

        Console.WriteLine("Config validation: failed");
        foreach (var error in result.Errors)
        {
            Console.WriteLine($"- {error.Code}: {error.File} {error.Message}");
        }
    }

    public void WriteConfigShow(ConfigShowResult result)
    {
        if (_json)
        {
            var payload = new
            {
                result.Command,
                result.ProjectDir,
                result.ConfigPath,
                result.Config,
            };
            WriteJson(payload);
            return;
        }

        Console.WriteLine($"Config: project-dir={result.ProjectDir}");
        Console.WriteLine($"Config file: {result.ConfigPath}");
        Console.WriteLine($"Database: server={result.Config.Database.Server}; name={result.Config.Database.Name}; auth={result.Config.Database.Auth}");
        Console.WriteLine($"Options: parallelism={result.Config.Options.Parallelism}");
    }

    public void WriteInit(InitResult result)
    {
        if (_json)
        {
            var payload = new
            {
                result.Command,
                result.ProjectDir,
                result.Created,
                result.Skipped,
                connectionTest = result.ConnectionTest != null
                    ? new { result.ConnectionTest.Success, result.ConnectionTest.ErrorMessage }
                    : (object?)null,
                nextSteps = result.NextSteps
            };
            WriteJson(payload);
            return;
        }

        Console.WriteLine($"Init: project-dir={result.ProjectDir}");
        Console.WriteLine("Created:");
        if (result.Created.Count == 0)
        {
            Console.WriteLine("  none");
        }
        else
        {
            foreach (var created in result.Created)
            {
                Console.WriteLine($"  {created}");
            }
        }

        Console.WriteLine("Skipped:");
        if (result.Skipped.Count == 0)
        {
            Console.WriteLine("  none");
        }
        else
        {
            foreach (var skipped in result.Skipped)
            {
                Console.WriteLine($"  {skipped}");
            }
        }

        if (result.ConnectionTest != null)
        {
            Console.WriteLine(result.ConnectionTest.Success
                ? "Connection test: ok"
                : "Connection test: failed");
            if (!result.ConnectionTest.Success && !string.IsNullOrWhiteSpace(result.ConnectionTest.ErrorMessage))
            {
                Console.WriteLine($"  {result.ConnectionTest.ErrorMessage}");
            }
        }

        if (result.NextSteps != null && result.NextSteps.Count > 0)
        {
            Console.WriteLine("Next steps:");
            foreach (var step in result.NextSteps)
            {
                Console.WriteLine($"  {step}");
            }
        }
    }

    public void WriteConfigValidate(ConfigValidateResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        if (result.Valid)
        {
            Console.WriteLine("Config validation: ok");
            return;
        }

        Console.WriteLine("Config validation: failed");
        foreach (var error in result.Errors)
        {
            Console.WriteLine($"- {error.Code}: {error.File} {error.Message}");
        }
    }

    public void WriteStatus(StatusResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Status: target={result.Target}");
        Console.WriteLine($"Schema: Added={result.Summary.Schema.Added}  Changed={result.Summary.Schema.Changed}  Deleted={result.Summary.Schema.Deleted}");
        Console.WriteLine($"Data:   Added={result.Summary.Data.Added}  Changed={result.Summary.Data.Changed}  Deleted={result.Summary.Data.Deleted}");
        Console.WriteLine();

        WriteStatusSection(result.Objects, "added", "Added:");
        WriteStatusSection(result.Objects, "changed", "Changed:");
        WriteStatusSection(result.Objects, "deleted", "Deleted:");
        WriteWarnings(result.Warnings);
    }

    public void WriteDiff(DiffResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        if (!string.IsNullOrWhiteSpace(result.Object))
        {
            Console.WriteLine($"Diff: {result.Object}");
        }
        else
        {
            Console.WriteLine($"Diff: target={result.Target}");
        }

        if (string.IsNullOrWhiteSpace(result.Diff))
        {
            Console.WriteLine("No differences.");
        }
        else
        {
            Console.WriteLine(result.Diff);
        }

        WriteWarnings(result.Warnings);
    }

    public void WritePull(PullResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Pull: project-dir={result.ProjectDir}");
        Console.WriteLine($"Schema: Created={result.Summary.Schema.Created}  Updated={result.Summary.Schema.Updated}  Deleted={result.Summary.Schema.Deleted}  Unchanged={result.Summary.Schema.Unchanged}");
        Console.WriteLine($"Data:   Created={result.Summary.Data.Created}  Updated={result.Summary.Data.Updated}  Deleted={result.Summary.Data.Deleted}  Unchanged={result.Summary.Data.Unchanged}");
        Console.WriteLine();
        WritePullSection(result.Objects, "created", "Created:");
        WritePullSection(result.Objects, "updated", "Updated:");
        WritePullSection(result.Objects, "deleted", "Deleted:");
        WriteWarnings(result.Warnings);
    }

    public void WriteError(ErrorResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Error: {result.Error.Message}");
        if (!string.IsNullOrWhiteSpace(result.Error.File))
        {
            Console.WriteLine($"File: {result.Error.File}");
        }

        if (!string.IsNullOrWhiteSpace(result.Error.Detail))
        {
            Console.WriteLine($"Detail: {result.Error.Detail}");
        }

        if (!string.IsNullOrWhiteSpace(result.Error.Hint))
        {
            Console.WriteLine($"Hint: {result.Error.Hint}");
        }
    }

    public void WriteDataTrack(DataTrackResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Data track: pattern={result.Pattern}");
        WriteTableList("Matched tables:", result.MatchedTables);
        WriteTableList("Tracked tables:", result.TrackedTables);
        if (result.Cancelled)
        {
            Console.WriteLine("Status: cancelled");
        }
        else if (result.Changed)
        {
            Console.WriteLine("Status: updated");
        }
        else
        {
            Console.WriteLine("Status: no changes");
        }
    }

    public void WriteDataUntrack(DataUntrackResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Data untrack: pattern={result.Pattern}");
        WriteTableList("Matched tables:", result.MatchedTables);
        WriteTableList("Tracked tables:", result.TrackedTables);
        if (result.Cancelled)
        {
            Console.WriteLine("Status: cancelled");
        }
        else if (result.Changed)
        {
            Console.WriteLine("Status: updated");
        }
        else
        {
            Console.WriteLine("Status: no changes");
        }
    }

    public void WriteDataList(DataListResult result)
    {
        if (_json)
        {
            WriteJson(result);
            return;
        }

        Console.WriteLine($"Data list: project-dir={result.ProjectDir}");
        WriteTableList("Tracked tables:", result.TrackedTables);
    }

    private static void WriteStatusSection(
        IReadOnlyList<StatusObject> objects,
        string changeKind,
        string heading)
    {
        var items = objects
            .Where(item => string.Equals(item.Change, changeKind, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (items.Count == 0)
        {
            return;
        }

        Console.WriteLine(heading);
        foreach (var item in items)
        {
            Console.WriteLine($"  {FormatObjectName(item.Name, item.Type)}");
        }
    }

    private static void WritePullSection(
        IReadOnlyList<PullObject> objects,
        string changeKind,
        string heading)
    {
        var items = objects
            .Where(item => string.Equals(item.Change, changeKind, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (items.Count == 0)
        {
            return;
        }

        Console.WriteLine(heading);
        foreach (var item in items)
        {
            Console.WriteLine($"  {FormatObjectName(item.Name, item.Type)} ({item.Path})");
        }
    }

    private static void WriteTableList(string heading, IReadOnlyList<string> tables)
    {
        Console.WriteLine(heading);
        if (tables.Count == 0)
        {
            Console.WriteLine("  none");
            return;
        }

        foreach (var table in tables)
        {
            Console.WriteLine($"  {table}");
        }
    }

    private static string FormatObjectName(string name, string type)
        => string.Equals(type, "TableData", StringComparison.OrdinalIgnoreCase)
            ? $"data:{name}"
            : name;

    private static void WriteWarnings(IReadOnlyList<CommandWarning> warnings)
    {
        if (warnings.Count == 0)
        {
            return;
        }

        Console.WriteLine();
        Console.WriteLine("Warnings:");
        foreach (var warning in warnings)
        {
            Console.WriteLine($"- {warning.Code}: {warning.Message}");
        }
    }

    private static void WriteJson<T>(T payload)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };
        Console.WriteLine(JsonSerializer.Serialize(payload, options));
    }
}
