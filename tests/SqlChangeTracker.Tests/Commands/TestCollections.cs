using Xunit;

namespace SqlChangeTracker.Tests.Commands;

/// <summary>
/// Defines a collection for tests that manipulate process-wide global state
/// (Console.In, Console.Out, Environment.CurrentDirectory).
/// Tests in the same collection run sequentially, preventing interference.
/// </summary>
[CollectionDefinition("GlobalConsoleTests")]
public sealed class GlobalConsoleTestsCollection { }
