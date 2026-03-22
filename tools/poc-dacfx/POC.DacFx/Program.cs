using System.Text;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.Model;

var argsMap = ParseArgs(args);
var serverName = GetArg(argsMap, "--server", "localhost");
var databaseName = GetArg(argsMap, "--database", "SampleDatabase");
var sourceName = GetArg(argsMap, "--source", "");
var hasObjectListArg = argsMap.TryGetValue("--object-list", out var objectListArg) && !string.IsNullOrWhiteSpace(objectListArg);
var hasOutArg = argsMap.TryGetValue("--out", out var outArg) && !string.IsNullOrWhiteSpace(outArg);
var needsSourceDefaults = !hasObjectListArg || !hasOutArg || !string.IsNullOrWhiteSpace(sourceName);
LocalFixtureSourceResolver.FixtureSource? sourceConfig = null;
if (needsSourceDefaults)
{
    try
    {
        sourceConfig = LocalFixtureSourceResolver.Resolve(sourceName);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Failed to resolve local fixture source: {ex.Message}");
        return 2;
    }
}

var objectListPath = hasObjectListArg ? objectListArg! : sourceConfig!.ObjectListPath;
var outputRoot = hasOutArg ? outArg! : Path.Combine("local", "fixtures", "outputs", $"poc-out-dacfx-{sourceConfig!.Name}");
var diagnostics = HasFlag(argsMap, "--diagnostics");

if (!File.Exists(objectListPath))
{
    Console.Error.WriteLine($"Object list not found: {objectListPath}");
    return 1;
}

Directory.CreateDirectory(outputRoot);

var objects = ParseObjectList(objectListPath);
if (objects.Count == 0)
{
    Console.Error.WriteLine($"No objects found in: {objectListPath}");
    return 1;
}

var connString = $"Server={serverName};Database={databaseName};Integrated Security=True;TrustServerCertificate=True;Encrypt=False;";
var dacpacPath = Path.Combine(outputRoot, "poc.dacpac");
var dacServices = new DacServices(connString);

Console.WriteLine($"Extracting dacpac to {dacpacPath}");
dacServices.Extract(dacpacPath, databaseName, "POC.DacFx", new Version(1, 0, 0, 0), null);

using var model = new TSqlModel(dacpacPath, DacSchemaModelStorageType.Memory);

foreach (var obj in objects)
{
    var dacObj = ResolveDacObject(model, obj);
    if (dacObj == null)
    {
        Console.Error.WriteLine($"SKIP (not found): {obj.RelativePath}");
        continue;
    }

    if (diagnostics)
    {
        DumpRelations(dacObj);
    }

    var script = ScriptObject(dacObj, model);
    var outPath = Path.Combine(outputRoot, obj.Folder, obj.FileName);
    Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    WriteUtf8NoBom(outPath, script);
    Console.WriteLine($"Wrote {obj.Folder}/{obj.FileName}");
}

return 0;

static Dictionary<string, string> ParseArgs(string[] args)
{
    var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var i = 0; i < args.Length; i++)
    {
        var key = args[i];
        if (!key.StartsWith("--", StringComparison.Ordinal)) continue;
        var value = (i + 1) < args.Length ? args[i + 1] : "";
        if (value.StartsWith("--", StringComparison.Ordinal)) value = "";
        map[key] = value;
    }
    return map;
}

static string GetArg(Dictionary<string, string> map, string key, string fallback)
{
    return map.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;
}

static bool HasFlag(Dictionary<string, string> map, string key)
{
    return map.TryGetValue(key, out var value) && (value == "" || value.Equals("true", StringComparison.OrdinalIgnoreCase));
}

static List<ObjectEntry> ParseObjectList(string path)
{
    var items = new List<ObjectEntry>();
    foreach (var line in File.ReadAllLines(path, Encoding.UTF8))
    {
        var trim = line.Trim();
        if (trim.Length == 0 || trim.StartsWith("#", StringComparison.Ordinal)) continue;
        if (!trim.Contains(".sql", StringComparison.OrdinalIgnoreCase)) continue;
        trim = trim.TrimStart('-').Trim();
        if (!trim.EndsWith(".sql", StringComparison.OrdinalIgnoreCase)) continue;
        var split = trim.Split(new[] { '/' }, 2);
        if (split.Length != 2) continue;
        var folder = split[0];
        var file = split[1];
        var name = Path.GetFileNameWithoutExtension(file);
        var dot = name.IndexOf('.');
        if (dot < 1) continue;
        items.Add(new ObjectEntry(folder, file, name[..dot], name[(dot + 1)..], trim));
    }
    return items;
}

static TSqlObject? ResolveDacObject(TSqlModel model, ObjectEntry entry)
{
    IEnumerable<TSqlObject> candidates = entry.Folder switch
    {
        "Tables" => model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.Table),
        "Views" => model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.View),
        "Stored Procedures" => model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.Procedure),
        "Sequences" => model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.Sequence),
        "Functions" => GetFunctionCandidates(model),
        _ => Array.Empty<TSqlObject>()
    };

    return candidates.FirstOrDefault(obj => MatchesName(obj, entry));
}

static IEnumerable<TSqlObject> GetFunctionCandidates(TSqlModel model)
{
    return model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.ScalarFunction)
        .Concat(model.GetObjects(DacQueryScopes.UserDefined, ModelSchema.TableValuedFunction));
}

static bool MatchesName(TSqlObject obj, ObjectEntry entry)
{
    var parts = obj.Name.Parts;
    if (parts.Count < 2) return false;
    var schema = parts[^2];
    var name = parts[^1];
    return string.Equals(schema, entry.Schema, StringComparison.OrdinalIgnoreCase)
        && string.Equals(name, entry.Name, StringComparison.OrdinalIgnoreCase);
}

static string ScriptObject(TSqlObject obj, TSqlModel model)
{
    var parts = new List<string>();
    var objectType = GetObjectTypeName(obj);
    var includeSetOptions = objectType is "View" or "Procedure" or "ScalarFunction" or "TableValuedFunction" or "InlineTableValuedFunction";
    if (includeSetOptions)
    {
        var setOptions = BuildSetOptions(obj);
        if (!string.IsNullOrWhiteSpace(setOptions))
        {
            parts.Add(setOptions);
        }
    }

    parts.Add(NormalizeScript(obj.GetScript()));

    foreach (var related in GetRelatedObjects(obj, model))
    {
        parts.Add(NormalizeScript(related.GetScript()));
    }

    return string.Join("\nGO\n", parts.Where(p => !string.IsNullOrWhiteSpace(p))) + "\n";
}

static IEnumerable<TSqlObject> GetRelatedObjects(TSqlObject obj, TSqlModel model)
{
    var objectType = GetObjectTypeName(obj);
    var related = new List<TSqlObject>();

    if (objectType == "Table")
    {
        related.AddRange(FilterRelated(obj.GetReferencing(), IsTableRelatedType));

        // Include column-level extended properties if present.
        foreach (var column in FilterRelated(obj.GetReferenced(), IsColumn))
        {
            related.AddRange(FilterRelated(column.GetReferencing(), IsExtendedProperty));
        }
    }
    else
    {
        related.AddRange(FilterRelated(obj.GetReferencing(), IsModuleRelatedType));
    }

    return related
        .GroupBy(o => o.Name.ToString(), StringComparer.OrdinalIgnoreCase)
        .Select(g => g.First());
}

static IEnumerable<TSqlObject> FilterRelated(IEnumerable<TSqlObject> source, Func<TSqlObject, bool> predicate)
{
    foreach (var item in source)
    {
        if (predicate(item))
        {
            yield return item;
        }
    }
}

static bool IsTableRelatedType(TSqlObject obj)
{
    var type = GetObjectTypeName(obj);
    return type is "PrimaryKeyConstraint" or "ForeignKeyConstraint" or "UniqueConstraint" or "CheckConstraint"
        or "DefaultConstraint" or "Index" or "Permission" or "ExtendedProperty";
}

static bool IsModuleRelatedType(TSqlObject obj)
{
    return IsPermission(obj) || IsExtendedProperty(obj);
}

static bool IsColumn(TSqlObject obj)
{
    return GetObjectTypeName(obj).Equals("Column", StringComparison.OrdinalIgnoreCase);
}

static bool IsPermission(TSqlObject obj)
{
    var type = GetObjectTypeName(obj);
    return type.Equals("Permission", StringComparison.OrdinalIgnoreCase)
        || type.EndsWith("Permission", StringComparison.OrdinalIgnoreCase);
}

static bool IsExtendedProperty(TSqlObject obj)
{
    return GetObjectTypeName(obj).Equals("ExtendedProperty", StringComparison.OrdinalIgnoreCase);
}

static string BuildSetOptions(TSqlObject obj)
{
    var lines = new List<string>();
    var quotedId = TryGetSetOption(obj, isAnsiNulls: false);
    if (quotedId.HasValue)
    {
        lines.Add($"SET QUOTED_IDENTIFIER {(quotedId.Value ? "ON" : "OFF")}");
    }
    var ansiNulls = TryGetSetOption(obj, isAnsiNulls: true);
    if (ansiNulls.HasValue)
    {
        lines.Add($"SET ANSI_NULLS {(ansiNulls.Value ? "ON" : "OFF")}");
    }
    return lines.Count == 0 ? "" : string.Join("\n", lines);
}

static bool? TryGetSetOption(TSqlObject obj, bool isAnsiNulls)
{
    var prop = GetSetOptionProperty(obj, isAnsiNulls);
    if (prop == null) return null;
    return obj.GetProperty<bool>(prop);
}

static ModelPropertyClass? GetSetOptionProperty(TSqlObject obj, bool isAnsiNulls)
{
    return GetObjectTypeName(obj) switch
    {
        "View" => isAnsiNulls ? View.AnsiNullsOn : View.QuotedIdentifierOn,
        "Procedure" => isAnsiNulls ? Procedure.AnsiNullsOn : Procedure.QuotedIdentifierOn,
        "ScalarFunction" => isAnsiNulls ? ScalarFunction.AnsiNullsOn : ScalarFunction.QuotedIdentifierOn,
        "TableValuedFunction" => isAnsiNulls ? TableValuedFunction.AnsiNullsOn : TableValuedFunction.QuotedIdentifierOn,
        "InlineTableValuedFunction" => isAnsiNulls ? TableValuedFunction.AnsiNullsOn : TableValuedFunction.QuotedIdentifierOn,
        _ => null
    };
}

static string NormalizeScript(string script)
{
    return script.Replace("\r\n", "\n").TrimEnd('\n', '\r');
}

static string GetObjectTypeName(TSqlObject obj)
{
    return obj.ObjectType?.Name ?? obj.ObjectType?.ToString() ?? obj.GetType().Name;
}

static void DumpRelations(TSqlObject obj)
{
    Console.WriteLine($"[diag] Object: {obj.Name} Type: {GetObjectTypeName(obj)}");
    foreach (var rel in obj.GetReferencing())
    {
        Console.WriteLine($"[diag]  referencing: {GetObjectTypeName(rel)} {rel.Name}");
    }
    foreach (var rel in obj.GetReferenced())
    {
        Console.WriteLine($"[diag]  referenced: {GetObjectTypeName(rel)} {rel.Name}");
    }
}

static void WriteUtf8NoBom(string path, string content)
{
    var utf8 = new UTF8Encoding(false);
    File.WriteAllText(path, content, utf8);
}

record ObjectEntry(string Folder, string FileName, string Schema, string Name, string RelativePath);
