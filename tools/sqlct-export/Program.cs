using SqlChangeTracker.Config;

var argsMap = ParseArgs(args);
if (!argsMap.TryGetValue("--server", out var server) ||
    !argsMap.TryGetValue("--database", out var database) ||
    !argsMap.TryGetValue("--out", out var outputDir))
{
    Console.WriteLine("Usage: --server <name> --database <name> --out <path> [--compat <path>] [--source <name>] [--auth integrated|sql] [--user <u>] [--password <p>] [--trust-server-certificate]");
    return 2;
}

var sourceName = argsMap.TryGetValue("--source", out var sourceArg) ? sourceArg : string.Empty;
var hasCompatPath = argsMap.TryGetValue("--compat", out var compatArg) && !string.IsNullOrWhiteSpace(compatArg);
var compatDir = compatArg;
if (!hasCompatPath)
{
    try
    {
        var sourceConfig = LocalFixtureSourceResolver.Resolve(sourceName);
        compatDir = sourceConfig.ReferencePath;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Failed to resolve local fixture source: {ex.Message}");
        return 2;
    }
}

var auth = argsMap.TryGetValue("--auth", out var authValue) ? authValue : "integrated";
var user = argsMap.TryGetValue("--user", out var userValue) ? userValue : null;
var password = argsMap.TryGetValue("--password", out var passwordValue) ? passwordValue : null;
var trust = argsMap.ContainsKey("--trust-server-certificate");

var outputRoot = Path.GetFullPath(outputDir);
var stageRoot = Path.Combine(Path.GetTempPath(), $"sqlct-export-stage-{Guid.NewGuid():N}");

try
{
    Directory.CreateDirectory(stageRoot);
    SeedCoreCompatibilityFiles(compatDir, stageRoot);

    var config = SqlctConfigWriter.CreateDefault();
    config.Database.Server = server;
    config.Database.Name = database;
    config.Database.Auth = auth;
    config.Database.User = user ?? string.Empty;
    config.Database.Password = password ?? string.Empty;
    config.Database.TrustServerCertificate = trust;

    var configWriter = new SqlctConfigWriter();
    var configPath = SqlctConfigWriter.GetDefaultPath(stageRoot);
    var writeResult = configWriter.Write(configPath, config, overwriteExisting: true);
    if (!writeResult.Success)
    {
        Console.Error.WriteLine(writeResult.Error?.Detail ?? writeResult.Error?.Message ?? "Failed to write temporary sqlct config.");
        return writeResult.ExitCode;
    }

    var pullExitCode = SqlChangeTracker.Program.Main(["pull", "--project-dir", stageRoot]);
    if (pullExitCode != 0)
    {
        return pullExitCode;
    }

    ResetDirectory(outputRoot);
    CopyCoreOutput(stageRoot, outputRoot);
    return 0;
}
finally
{
    TryDeleteDirectory(stageRoot);
}

static Dictionary<string, string> ParseArgs(string[] args)
{
    var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var i = 0; i < args.Length; i++)
    {
        var key = args[i];
        if (!key.StartsWith("-", StringComparison.Ordinal))
        {
            continue;
        }

        if (key.StartsWith("--trust-server-certificate", StringComparison.OrdinalIgnoreCase))
        {
            map[key] = "true";
            continue;
        }

        if (i + 1 < args.Length)
        {
            map[key] = args[i + 1];
            i++;
        }
    }

    return map;
}

static void SeedCoreCompatibilityFiles(string? compatDir, string stageRoot)
{
    if (string.IsNullOrWhiteSpace(compatDir) || !Directory.Exists(compatDir))
    {
        return;
    }

    foreach (var folder in GetCoreObjectFolders())
    {
        var sourceFolder = Path.Combine(compatDir, folder);
        if (!Directory.Exists(sourceFolder))
        {
            continue;
        }

        var targetFolder = Path.Combine(stageRoot, folder);
        Directory.CreateDirectory(targetFolder);

        foreach (var sourceFile in Directory.EnumerateFiles(sourceFolder, "*.sql", SearchOption.TopDirectoryOnly))
        {
            var targetFile = Path.Combine(targetFolder, Path.GetFileName(sourceFile));
            File.Copy(sourceFile, targetFile, overwrite: true);
        }
    }
}

static void CopyCoreOutput(string stageRoot, string outputRoot)
{
    foreach (var folder in GetCoreObjectFolders())
    {
        var sourceFolder = Path.Combine(stageRoot, folder);
        if (!Directory.Exists(sourceFolder))
        {
            continue;
        }

        var targetFolder = Path.Combine(outputRoot, folder);
        Directory.CreateDirectory(targetFolder);

        foreach (var sourceFile in Directory.EnumerateFiles(sourceFolder, "*.sql", SearchOption.TopDirectoryOnly))
        {
            var targetFile = Path.Combine(targetFolder, Path.GetFileName(sourceFile));
            File.Copy(sourceFile, targetFile, overwrite: true);
        }
    }
}

static void ResetDirectory(string path)
{
    if (Directory.Exists(path))
    {
        Directory.Delete(path, recursive: true);
    }

    Directory.CreateDirectory(path);
}

static void TryDeleteDirectory(string path)
{
    try
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }
    catch (IOException)
    {
    }
    catch (UnauthorizedAccessException)
    {
    }
}

static IReadOnlyList<string> GetCoreObjectFolders()
    =>
    [
        "Tables",
        "Views",
        "Stored Procedures",
        "Functions",
        "Sequences"
    ];
