using Spectre.Console.Cli;
using SqlChangeTracker.Commands;
using SqlChangeTracker.Config;
using SqlChangeTracker.Sync;
using Xunit;

namespace SqlChangeTracker.Tests.Commands;

public sealed class StatusDiffPullCommandTests
{
    [Fact]
    public void StatusCommand_WhenNoDiff_ReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            StatusResult = CommandExecutionResult<StatusResult>.Ok(
                new StatusResult(
                    "status",
                    ".\\schema",
                    "db",
                    new StatusSummary(0, 0, 0),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new StatusCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("status"), new StatusCommandSettings { Target = "db" }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void StatusCommand_WhenDiffExists_ReturnsDiffExitCode()
    {
        var stub = new StubSyncCommandService
        {
            StatusResult = CommandExecutionResult<StatusResult>.Ok(
                new StatusResult(
                    "status",
                    ".\\schema",
                    "db",
                    new StatusSummary(1, 0, 0),
                    [new StatusObject("dbo.Customer", "Table", "added")],
                    []),
                ExitCodes.DiffExists)
        };

        var command = new StatusCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("status"), new StatusCommandSettings { Target = "db" }, default);

        Assert.Equal(ExitCodes.DiffExists, exitCode);
    }

    [Fact]
    public void StatusCommand_WhenWarningsPresent_KeepsServiceExitCode()
    {
        var stub = new StubSyncCommandService
        {
            StatusResult = CommandExecutionResult<StatusResult>.Ok(
                new StatusResult(
                    "status",
                    ".\\schema",
                    "db",
                    new StatusSummary(0, 0, 0),
                    [],
                    [new CommandWarning("unsupported_folder_entry", "skipped unsupported folder entry 'Data\\dbo.Customer_Data.sql'.")]),
                ExitCodes.Success)
        };

        var command = new StatusCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("status"), new StatusCommandSettings { Target = "db" }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void DiffCommand_WhenServiceReturnsError_PropagatesExitCode()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector."),
                ExitCodes.InvalidConfig)
        };

        var command = new DiffCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { ObjectName = "dbo" }, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
    }

    [Fact]
    public void DiffCommand_WhenDiffExists_ReturnsDiffExitCode()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Ok(
                new DiffResult(
                    "diff",
                    ".\\schema",
                    "db",
                    "dbo.Customer",
                    "--- db\n+++ folder\n@@\n-SELECT 1\n+SELECT 2",
                    []),
                ExitCodes.DiffExists)
        };

        var command = new DiffCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { ObjectName = "dbo.Customer" }, default);

        Assert.Equal(ExitCodes.DiffExists, exitCode);
    }

    [Fact]
    public void DiffCommand_WhenWarningsPresent_KeepsServiceExitCode()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Ok(
                new DiffResult(
                    "diff",
                    ".\\schema",
                    "db",
                    null,
                    string.Empty,
                    [new CommandWarning("unsupported_folder_entry", "skipped unsupported folder entry 'Security\\Roles\\db_datareader.sql'.")]),
                ExitCodes.Success)
        };

        var command = new DiffCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings(), default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WhenSuccess_ReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    new PullSummary(1, 1, 0, 2),
                    [new PullObject("dbo.Customer", "Table", "created", "Tables\\dbo.Customer.sql")],
                    []),
                ExitCodes.Success)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings(), default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WhenWarningsPresent_KeepsServiceExitCode()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    new PullSummary(0, 0, 0, 1),
                    [],
                    [new CommandWarning("unsupported_folder_entry", "skipped unsupported folder entry 'Storage\\Partition Functions\\DATA_Loadset_PF.sql'.")]),
                ExitCodes.Success)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings(), default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WhenServiceReturnsError_PropagatesExitCode()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Failure(
                new ErrorInfo(ErrorCodes.ConnectionFailed, "failed to connect to SQL Server."),
                ExitCodes.ConnectionFailure)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings(), default);

        Assert.Equal(ExitCodes.ConnectionFailure, exitCode);
    }

    [Fact]
    public void StatusCommand_WithNoProgressFlag_ReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            StatusResult = CommandExecutionResult<StatusResult>.Ok(
                new StatusResult(
                    "status",
                    ".\\schema",
                    "db",
                    new StatusSummary(0, 0, 0),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new StatusCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("status"), new StatusCommandSettings { Target = "db", NoProgress = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void StatusCommand_WithJsonFlag_SuppressesProgressAndReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            StatusResult = CommandExecutionResult<StatusResult>.Ok(
                new StatusResult(
                    "status",
                    ".\\schema",
                    "db",
                    new StatusSummary(0, 0, 0),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new StatusCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("status"), new StatusCommandSettings { Target = "db", Json = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void DiffCommand_WithNoProgressFlag_ReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Ok(
                new DiffResult(
                    "diff",
                    ".\\schema",
                    "db",
                    null,
                    string.Empty,
                    []),
                ExitCodes.Success)
        };

        var command = new DiffCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { NoProgress = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void DiffCommand_WithJsonFlag_SuppressesProgressAndReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Ok(
                new DiffResult(
                    "diff",
                    ".\\schema",
                    "db",
                    null,
                    string.Empty,
                    []),
                ExitCodes.Success)
        };

        var command = new DiffCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { Json = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WithNoProgressFlag_ReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    new PullSummary(0, 0, 0, 1),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings { NoProgress = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WithJsonFlag_SuppressesProgressAndReturnsSuccess()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    new PullSummary(0, 0, 0, 1),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings { Json = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    private static CommandContext CreateContext(string name)
        => new([], new EmptyRemainingArguments(), name, null!);

    private sealed class EmptyRemainingArguments : IRemainingArguments
    {
        public ILookup<string, string?> Parsed { get; } =
            Array.Empty<KeyValuePair<string, string?>>().ToLookup(item => item.Key, item => item.Value);

        public IReadOnlyList<string> Raw { get; } = Array.Empty<string>();
    }

    private sealed class StubSyncCommandService : ISyncCommandService
    {
        public CommandExecutionResult<StatusResult> StatusResult { get; set; } =
            CommandExecutionResult<StatusResult>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "status not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<DiffResult> DiffResult { get; set; } =
            CommandExecutionResult<DiffResult>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "diff not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<PullResult> PullResult { get; set; } =
            CommandExecutionResult<PullResult>.Failure(new ErrorInfo(ErrorCodes.ExecutionFailed, "pull not configured"), ExitCodes.ExecutionFailure);

        public CommandExecutionResult<StatusResult> RunStatus(string? projectDir, string? target)
            => StatusResult;

        public CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectName)
            => DiffResult;

        public CommandExecutionResult<PullResult> RunPull(string? projectDir)
            => PullResult;
    }
}
