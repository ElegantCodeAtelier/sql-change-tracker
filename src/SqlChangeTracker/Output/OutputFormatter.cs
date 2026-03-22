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
        Console.WriteLine($"Options: orderByDependencies={result.Config.Options.OrderByDependencies.ToString().ToLowerInvariant()}");
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
                result.Skipped
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
        Console.WriteLine($"Added: {result.Summary.Added}  Changed: {result.Summary.Changed}  Deleted: {result.Summary.Deleted}");
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
        Console.WriteLine($"Created: {result.Summary.Created}  Updated: {result.Summary.Updated}  Deleted: {result.Summary.Deleted}  Unchanged: {result.Summary.Unchanged}");
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

    private static void WriteStatusSection(
        IReadOnlyList<StatusObject> objects,
        string changeKind,
        string heading)
    {
        var items = objects
            .Where(item => string.Equals(item.Change, changeKind, StringComparison.OrdinalIgnoreCase))
            .Select(item => item.Name)
            .ToList();

        if (items.Count == 0)
        {
            return;
        }

        Console.WriteLine(heading);
        foreach (var item in items)
        {
            Console.WriteLine($"  {item}");
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
            Console.WriteLine($"  {item.Name} ({item.Path})");
        }
    }

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
