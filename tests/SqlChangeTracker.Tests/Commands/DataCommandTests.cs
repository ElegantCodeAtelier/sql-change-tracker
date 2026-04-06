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
        public bool ApplyTrackCalled { get; private set; }

        public bool ApplyUntrackCalled { get; private set; }

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

        public CommandExecutionResult<DataTrackPlan> PrepareTrack(string? projectDir, string pattern)
            => TrackPlanResult;

        public CommandExecutionResult<DataTrackResult> ApplyTrack(DataTrackPlan plan)
        {
            ApplyTrackCalled = true;
            return TrackApplyResult;
        }

        public CommandExecutionResult<DataUntrackPlan> PrepareUntrack(string? projectDir, string pattern)
            => UntrackPlanResult;

        public CommandExecutionResult<DataUntrackResult> ApplyUntrack(DataUntrackPlan plan)
        {
            ApplyUntrackCalled = true;
            return UntrackApplyResult;
        }

        public CommandExecutionResult<DataListResult> RunList(string? projectDir)
            => ListResult;
    }
}
