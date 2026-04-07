using Spectre.Console.Cli;
using SqlChangeTracker.Commands;
using SqlChangeTracker.Config;
using SqlChangeTracker.Data;
using SqlChangeTracker.Sync;
using Xunit;

namespace SqlChangeTracker.Tests.Commands;

public sealed class DataCommandTests
{
    [Fact]
    public void DataTrackCommand_WhenNoMatches_ReturnsSuccessWithoutConfirmation()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    [],
                    ["dbo.Customer"],
                    ["dbo.Customer"],
                    false),
                ExitCodes.Success)
        };

        var command = new DataTrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings { Pattern = "dbo.*" }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
        Assert.False(stub.ApplyTrackCalled);
    }

    [Fact]
    public void DataTrackCommand_WhenDeclined_ReturnsSuccessWithoutApplying()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    ["dbo.Customer", "dbo.Order"],
                    ["dbo.Customer"],
                    ["dbo.Customer", "dbo.Order"],
                    true),
                ExitCodes.Success)
        };

        var originalInput = Console.In;
        try
        {
            Console.SetIn(new StringReader("n" + Environment.NewLine));

            var command = new DataTrackCommand { DataTrackingService = stub };
            var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings { Pattern = "dbo.*" }, default);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.False(stub.ApplyTrackCalled);
        }
        finally
        {
            Console.SetIn(originalInput);
        }
    }

    [Fact]
    public void DataTrackCommand_WhenConfirmationUnavailable_ReturnsExecutionFailure()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    ["dbo.Customer"],
                    [],
                    ["dbo.Customer"],
                    true),
                ExitCodes.Success)
        };

        var originalInput = Console.In;
        try
        {
            Console.SetIn(new StringReader(string.Empty));

            var command = new DataTrackCommand { DataTrackingService = stub };
            var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings { Pattern = "dbo.*" }, default);

            Assert.Equal(ExitCodes.ExecutionFailure, exitCode);
            Assert.False(stub.ApplyTrackCalled);
        }
        finally
        {
            Console.SetIn(originalInput);
        }
    }

    [Fact]
    public void DataUntrackCommand_WhenNoMatches_ReturnsSuccessWithoutConfirmation()
    {
        var stub = new StubDataTrackingService
        {
            UntrackPlanResult = CommandExecutionResult<DataUntrackPlan>.Ok(
                new DataUntrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "*.Missing",
                    [],
                    ["dbo.Customer"],
                    ["dbo.Customer"],
                    false),
                ExitCodes.Success)
        };

        var command = new DataUntrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings { Pattern = "*.Missing" }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
        Assert.False(stub.ApplyUntrackCalled);
    }

    [Fact]
    public void DataUntrackCommand_WhenDeclined_WritesPreviewAndDoesNotApply()
    {
        var stub = new StubDataTrackingService
        {
            UntrackPlanResult = CommandExecutionResult<DataUntrackPlan>.Ok(
                new DataUntrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    ["dbo.Customer"],
                    ["dbo.Customer", "dbo.Order"],
                    ["dbo.Order"],
                    true),
                ExitCodes.Success)
        };

        var originalInput = Console.In;
        var originalOut = Console.Out;
        var stdout = new StringWriter();

        try
        {
            Console.SetIn(new StringReader("n" + Environment.NewLine));
            Console.SetOut(stdout);

            var command = new DataUntrackCommand { DataTrackingService = stub };
            var exitCode = command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings { Pattern = "dbo.*" }, default);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.False(stub.ApplyUntrackCalled);
            Assert.Contains("Matching tracked tables:", stdout.ToString());
            Assert.Contains("Untrack these tables? [y/N]:", stdout.ToString());
        }
        finally
        {
            Console.SetIn(originalInput);
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public void DataTrackCommand_WithJsonFlag_WritesPromptToStdErrAndJsonToStdOut()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    ["dbo.Customer"],
                    [],
                    ["dbo.Customer"],
                    true),
                ExitCodes.Success),
            TrackApplyResult = CommandExecutionResult<DataTrackResult>.Ok(
                new DataTrackResult("data track", ".\\schema", "dbo.*", true, false, ["dbo.Customer"], ["dbo.Customer"]),
                ExitCodes.Success)
        };

        var originalInput = Console.In;
        var originalOut = Console.Out;
        var originalError = Console.Error;
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        try
        {
            Console.SetIn(new StringReader("y" + Environment.NewLine));
            Console.SetOut(stdout);
            Console.SetError(stderr);

            var command = new DataTrackCommand { DataTrackingService = stub };
            var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings { Pattern = "dbo.*", Json = true }, default);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.True(stub.ApplyTrackCalled);
            Assert.Contains("\"command\": \"data track\"", stdout.ToString());
            Assert.DoesNotContain("Track these tables?", stdout.ToString());
            Assert.Contains("Matching tables:", stderr.ToString());
            Assert.Contains("Track these tables? [y/N]:", stderr.ToString());
        }
        finally
        {
            Console.SetIn(originalInput);
            Console.SetOut(originalOut);
            Console.SetError(originalError);
        }
    }

    [Fact]
    public void ProgramMain_DataList_RoutesAndReturnsSuccess()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);
            Assert.True(write.Success);

            var exitCode = Program.Main(["data", "list", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.Success, exitCode);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void ProgramMain_DataUntrack_RoutesAndReturnsSuccess()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);
            Assert.True(write.Success);

            var exitCode = Program.Main(["data", "untrack", "dbo.Customer", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.Success, exitCode);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void ProgramMain_DataTrack_RoutesAndReturnsInvalidConfigWhenProjectMissing()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "missing");

            var exitCode = Program.Main(["data", "track", "dbo.Customer", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void DataTrackCommand_WithNoSelector_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataTrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings(), default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareTrackCalled);
    }

    [Fact]
    public void DataTrackCommand_WithBothPositionalAndObject_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataTrackCommand { DataTrackingService = stub };
        var settings = new DataTrackCommandSettings { Pattern = "dbo.*", ObjectPattern = "dbo.*" };
        var exitCode = command.Execute(CreateContext("track"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareTrackCalled);
    }

    [Fact]
    public void DataTrackCommand_WithBothObjectAndFilter_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataTrackCommand { DataTrackingService = stub };
        var settings = new DataTrackCommandSettings { ObjectPattern = "dbo.*", FilterPattern = "^dbo\\." };
        var exitCode = command.Execute(CreateContext("track"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareTrackCalled);
    }

    [Fact]
    public void DataTrackCommand_WithBothPositionalAndFilter_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataTrackCommand { DataTrackingService = stub };
        var settings = new DataTrackCommandSettings { Pattern = "dbo.*", FilterPattern = "^dbo\\." };
        var exitCode = command.Execute(CreateContext("track"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareTrackCalled);
    }

    [Fact]
    public void DataTrackCommand_WithObjectOption_PassesObjectPatternToService()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    [],
                    [],
                    [],
                    false),
                ExitCodes.Success)
        };

        var command = new DataTrackCommand { DataTrackingService = stub };
        command.Execute(CreateContext("track"), new DataTrackCommandSettings { ObjectPattern = "dbo.*" }, default);

        Assert.True(stub.PrepareTrackCalled);
        Assert.Equal("dbo.*", stub.LastObjectPattern);
        Assert.Null(stub.LastFilterPattern);
    }

    [Fact]
    public void DataTrackCommand_WithFilterOption_PassesFilterPatternToService()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Ok(
                new DataTrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "^dbo\\.",
                    [],
                    [],
                    [],
                    false),
                ExitCodes.Success)
        };

        var command = new DataTrackCommand { DataTrackingService = stub };
        command.Execute(CreateContext("track"), new DataTrackCommandSettings { FilterPattern = "^dbo\\." }, default);

        Assert.True(stub.PrepareTrackCalled);
        Assert.Null(stub.LastObjectPattern);
        Assert.Equal("^dbo\\.", stub.LastFilterPattern);
    }

    [Fact]
    public void DataTrackCommand_WithInvalidFilterPattern_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService
        {
            TrackPlanResult = CommandExecutionResult<DataTrackPlan>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: "'[invalid' is not a valid regular expression."),
                ExitCodes.InvalidConfig)
        };

        var command = new DataTrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("track"), new DataTrackCommandSettings { FilterPattern = "[invalid" }, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
    }

    [Fact]
    public void DataUntrackCommand_WithNoSelector_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataUntrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings(), default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareUntrackCalled);
    }

    [Fact]
    public void DataUntrackCommand_WithBothObjectAndFilter_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService();
        var command = new DataUntrackCommand { DataTrackingService = stub };
        var settings = new DataUntrackCommandSettings { ObjectPattern = "dbo.*", FilterPattern = "^dbo\\." };
        var exitCode = command.Execute(CreateContext("untrack"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        Assert.False(stub.PrepareUntrackCalled);
    }

    [Fact]
    public void DataUntrackCommand_WithObjectOption_PassesObjectPatternToService()
    {
        var stub = new StubDataTrackingService
        {
            UntrackPlanResult = CommandExecutionResult<DataUntrackPlan>.Ok(
                new DataUntrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "dbo.*",
                    [],
                    [],
                    [],
                    false),
                ExitCodes.Success)
        };

        var command = new DataUntrackCommand { DataTrackingService = stub };
        command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings { ObjectPattern = "dbo.*" }, default);

        Assert.True(stub.PrepareUntrackCalled);
        Assert.Equal("dbo.*", stub.LastUntrackObjectPattern);
        Assert.Null(stub.LastUntrackFilterPattern);
    }

    [Fact]
    public void DataUntrackCommand_WithFilterOption_PassesFilterPatternToService()
    {
        var stub = new StubDataTrackingService
        {
            UntrackPlanResult = CommandExecutionResult<DataUntrackPlan>.Ok(
                new DataUntrackPlan(
                    ".\\schema",
                    ".\\schema",
                    ".\\schema\\sqlct.config.json",
                    "^dbo\\.",
                    [],
                    [],
                    [],
                    false),
                ExitCodes.Success)
        };

        var command = new DataUntrackCommand { DataTrackingService = stub };
        command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings { FilterPattern = "^dbo\\." }, default);

        Assert.True(stub.PrepareUntrackCalled);
        Assert.Null(stub.LastUntrackObjectPattern);
        Assert.Equal("^dbo\\.", stub.LastUntrackFilterPattern);
    }

    [Fact]
    public void DataUntrackCommand_WithInvalidFilterPattern_ReturnsInvalidConfig()
    {
        var stub = new StubDataTrackingService
        {
            UntrackPlanResult = CommandExecutionResult<DataUntrackPlan>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: "'[invalid' is not a valid regular expression."),
                ExitCodes.InvalidConfig)
        };

        var command = new DataUntrackCommand { DataTrackingService = stub };
        var exitCode = command.Execute(CreateContext("untrack"), new DataUntrackCommandSettings { FilterPattern = "[invalid" }, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
    }

    [Fact]
    public void PrepareUntrack_WithFilterPattern_MatchesTrackedTablesByRegex()
    {
        var tempDir = CreateTempDir();
        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            config.Data.TrackedTables = ["dbo.Customer", "dbo.Order", "Sales.Invoice"];
            new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);

            var service = new DataTrackingService();
            var result = service.PrepareUntrack(projectDir, objectPattern: null, filterPattern: "^dbo\\.");

            Assert.True(result.Success);
            Assert.Equal(["dbo.Customer", "dbo.Order"], result.Payload!.MatchedTables);
            Assert.Equal(["Sales.Invoice"], result.Payload.NextTrackedTables);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void PrepareUntrack_WithFilterPattern_IsCaseInsensitive()
    {
        var tempDir = CreateTempDir();
        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            config.Data.TrackedTables = ["dbo.Customer", "DBO.Order"];
            new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);

            var service = new DataTrackingService();
            var result = service.PrepareUntrack(projectDir, objectPattern: null, filterPattern: "DBO\\.Customer");

            Assert.True(result.Success);
            Assert.Single(result.Payload!.MatchedTables);
            Assert.Equal("dbo.Customer", result.Payload.MatchedTables[0], StringComparer.OrdinalIgnoreCase);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void PrepareUntrack_WithInvalidFilterPattern_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();
        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);

            var service = new DataTrackingService();
            var result = service.PrepareUntrack(projectDir, objectPattern: null, filterPattern: "[invalid");

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.Equal("invalid filter pattern.", result.Error!.Message);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void PrepareUntrack_WithFilterPattern_NoMatchReturnsEmptyList()
    {
        var tempDir = CreateTempDir();
        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var config = SqlctConfigWriter.CreateDefault();
            config.Data.TrackedTables = ["dbo.Customer"];
            new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config);

            var service = new DataTrackingService();
            var result = service.PrepareUntrack(projectDir, objectPattern: null, filterPattern: "^Sales\\.");

            Assert.True(result.Success);
            Assert.Empty(result.Payload!.MatchedTables);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    private static CommandContext CreateContext(string name)
        => new([], new EmptyRemainingArguments(), name, null!);

    private static string CreateTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void CleanupTempDir(string tempDir)
    {
        if (Directory.Exists(tempDir))
        {
            Directory.Delete(tempDir, true);
        }
    }

    private sealed class EmptyRemainingArguments : IRemainingArguments
    {
        public ILookup<string, string?> Parsed { get; } =
            Array.Empty<KeyValuePair<string, string?>>().ToLookup(item => item.Key, item => item.Value);

        public IReadOnlyList<string> Raw { get; } = Array.Empty<string>();
    }

    private sealed class StubDataTrackingService : IDataTrackingService
    {
        public bool PrepareTrackCalled { get; private set; }

        public bool ApplyTrackCalled { get; private set; }

        public bool PrepareUntrackCalled { get; private set; }

        public bool ApplyUntrackCalled { get; private set; }

        public string? LastObjectPattern { get; private set; }

        public string? LastFilterPattern { get; private set; }

        public string? LastUntrackObjectPattern { get; private set; }

        public string? LastUntrackFilterPattern { get; private set; }

        public CommandExecutionResult<DataTrackPlan> TrackPlanResult { get; set; } =
            CommandExecutionResult<DataTrackPlan>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "track not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<DataTrackResult> TrackApplyResult { get; set; } =
            CommandExecutionResult<DataTrackResult>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "track apply not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<DataUntrackPlan> UntrackPlanResult { get; set; } =
            CommandExecutionResult<DataUntrackPlan>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "untrack not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<DataUntrackResult> UntrackApplyResult { get; set; } =
            CommandExecutionResult<DataUntrackResult>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "untrack apply not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<DataListResult> ListResult { get; set; } =
            CommandExecutionResult<DataListResult>.Ok(new DataListResult("data list", ".\\schema", []), ExitCodes.Success);

        public CommandExecutionResult<DataTrackPlan> PrepareTrack(string? projectDir, string? objectPattern, string? filterPattern)
        {
            PrepareTrackCalled = true;
            LastObjectPattern = objectPattern;
            LastFilterPattern = filterPattern;
            return TrackPlanResult;
        }

        public CommandExecutionResult<DataTrackResult> ApplyTrack(DataTrackPlan plan)
        {
            ApplyTrackCalled = true;
            return TrackApplyResult;
        }

        public CommandExecutionResult<DataUntrackPlan> PrepareUntrack(string? projectDir, string? objectPattern, string? filterPattern)
        {
            PrepareUntrackCalled = true;
            LastUntrackObjectPattern = objectPattern;
            LastUntrackFilterPattern = filterPattern;
            return UntrackPlanResult;
        }

        public CommandExecutionResult<DataUntrackResult> ApplyUntrack(DataUntrackPlan plan)
        {
            ApplyUntrackCalled = true;
            return UntrackApplyResult;
        }

        public CommandExecutionResult<DataListResult> RunList(string? projectDir)
            => ListResult;
    }
}
