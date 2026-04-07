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
                    EmptyStatusSummary(),
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
                    SchemaStatusSummary(1, 0, 0),
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
                    EmptyStatusSummary(),
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
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { ObjectSelector = "dbo" }, default);

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
        var exitCode = command.Execute(CreateContext("diff"), new DiffCommandSettings { ObjectSelector = "dbo.Customer" }, default);

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
                    [new CommandWarning("unsupported_folder_entry", "skipped unsupported folder entry 'Custom\\dbo.Legacy.sql'.")]),
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
                    SchemaPullSummary(1, 1, 0, 2),
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
                    EmptyPullSummary(schemaUnchanged: 1),
                    [],
                    [new CommandWarning("unsupported_folder_entry", "skipped unsupported folder entry 'Data\\dbo.Customer_Data.sql'.")]),
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
                    EmptyStatusSummary(),
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
                    EmptyStatusSummary(),
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
    public void DiffCommand_WithFilterPatterns_PassesPatternsToService()
    {
        string[]? capturedPatterns = null;
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
                ExitCodes.Success),
            OnRunDiff = (_, _, _, patterns) => capturedPatterns = patterns
        };

        var command = new DiffCommand { SyncService = stub };
        var settings = new DiffCommandSettings { FilterPatterns = ["dbo\\.Customer", "dbo\\..*"] };
        var exitCode = command.Execute(CreateContext("diff"), settings, default);

        Assert.Equal(ExitCodes.Success, exitCode);
        Assert.NotNull(capturedPatterns);
        Assert.Equal(2, capturedPatterns!.Length);
        Assert.Equal("dbo\\.Customer", capturedPatterns[0]);
        Assert.Equal("dbo\\..*", capturedPatterns[1]);
    }

    [Fact]
    public void DiffCommand_WithInvalidFilterPattern_ReturnsInvalidConfigError()
    {
        var stub = new StubSyncCommandService
        {
            DiffResult = CommandExecutionResult<DiffResult>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: "'[invalid' is not a valid regular expression."),
                ExitCodes.InvalidConfig)
        };

        var command = new DiffCommand { SyncService = stub };
        var settings = new DiffCommandSettings { FilterPatterns = ["[invalid"] };
        var exitCode = command.Execute(CreateContext("diff"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
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
                    EmptyPullSummary(schemaUnchanged: 1),
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
                    EmptyPullSummary(schemaUnchanged: 1),
                    [],
                    []),
                ExitCodes.Success)
        };

        var command = new PullCommand { SyncService = stub };
        var exitCode = command.Execute(CreateContext("pull"), new PullCommandSettings { Json = true }, default);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void PullCommand_WithObjectPatterns_PassesPatternsToService()
    {
        string? capturedSelector = null;
        string[]? capturedPatterns = null;
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    EmptyPullSummary(schemaUnchanged: 1),
                    [],
                    []),
                ExitCodes.Success),
            OnRunPull = (_, selector, patterns, _) =>
            {
                capturedSelector = selector;
                capturedPatterns = patterns;
            }
        };

        var command = new PullCommand { SyncService = stub };
        var settings = new PullCommandSettings { FilterPatterns = ["dbo\\.Customer", "dbo\\..*"] };
        var exitCode = command.Execute(CreateContext("pull"), settings, default);

        Assert.Equal(ExitCodes.Success, exitCode);
        Assert.Null(capturedSelector);
        Assert.NotNull(capturedPatterns);
        Assert.Equal(2, capturedPatterns!.Length);
        Assert.Equal("dbo\\.Customer", capturedPatterns[0]);
        Assert.Equal("dbo\\..*", capturedPatterns[1]);
    }

    [Fact]
    public void PullCommand_WithObjectSelector_PassesSelectorToService()
    {
        string? capturedSelector = null;
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Ok(
                new PullResult(
                    "pull",
                    ".\\schema",
                    EmptyPullSummary(schemaUnchanged: 1),
                    [],
                    []),
                ExitCodes.Success),
            OnRunPull = (_, selector, _, _) => capturedSelector = selector
        };

        var command = new PullCommand { SyncService = stub };
        var settings = new PullCommandSettings { ObjectSelector = "dbo.Customer" };
        var exitCode = command.Execute(CreateContext("pull"), settings, default);

        Assert.Equal(ExitCodes.Success, exitCode);
        Assert.Equal("dbo.Customer", capturedSelector);
    }

    [Fact]
    public void PullCommand_WithInvalidObjectPattern_ReturnsInvalidConfigError()
    {
        var stub = new StubSyncCommandService
        {
            PullResult = CommandExecutionResult<PullResult>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: "'[invalid' is not a valid regular expression."),
                ExitCodes.InvalidConfig)
        };

        var command = new PullCommand { SyncService = stub };
        var settings = new PullCommandSettings { FilterPatterns = ["[invalid"] };
        var exitCode = command.Execute(CreateContext("pull"), settings, default);

        Assert.Equal(ExitCodes.InvalidConfig, exitCode);
    }

    private static CommandContext CreateContext(string name)
        => new([], new EmptyRemainingArguments(), name, null!);

    private static StatusSummary EmptyStatusSummary()
        => new(new ChangeSummary(0, 0, 0), new ChangeSummary(0, 0, 0));

    private static StatusSummary SchemaStatusSummary(int added, int changed, int deleted)
        => new(new ChangeSummary(added, changed, deleted), new ChangeSummary(0, 0, 0));

    private static PullSummary EmptyPullSummary(
        int schemaCreated = 0,
        int schemaUpdated = 0,
        int schemaDeleted = 0,
        int schemaUnchanged = 0,
        int dataCreated = 0,
        int dataUpdated = 0,
        int dataDeleted = 0,
        int dataUnchanged = 0)
        => new(
            new PullChangeSummary(schemaCreated, schemaUpdated, schemaDeleted, schemaUnchanged),
            new PullChangeSummary(dataCreated, dataUpdated, dataDeleted, dataUnchanged));

    private static PullSummary SchemaPullSummary(int created, int updated, int deleted, int unchanged)
        => EmptyPullSummary(schemaCreated: created, schemaUpdated: updated, schemaDeleted: deleted, schemaUnchanged: unchanged);

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

        public Action<string?, string?, string?, string[]?>? OnRunDiff { get; set; }

        public Action<string?, string?, string[]?, Action<string>?>? OnRunPull { get; set; }

        public CommandExecutionResult<StatusResult> RunStatus(string? projectDir, string? target, Action<string>? progress = null)
            => StatusResult;

        public CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectSelector, string[]? filterPatterns = null, Action<string>? progress = null)
        {
            OnRunDiff?.Invoke(projectDir, target, objectSelector, filterPatterns);
            return DiffResult;
        }

        public CommandExecutionResult<PullResult> RunPull(string? projectDir, string? objectSelector = null, string[]? filterPatterns = null, Action<string>? progress = null)
        {
            OnRunPull?.Invoke(projectDir, objectSelector, filterPatterns, progress);
            return PullResult;
        }
    }
}
