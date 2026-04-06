using System.IO;

namespace SqlChangeTracker.Commands;

internal static class ConfirmationPrompt
{
    public static bool? WritePreviewAndConfirm(bool json, string heading, IReadOnlyList<string> tables, string prompt)
    {
        var writer = json ? Console.Error : Console.Out;
        WritePreview(writer, heading, tables);
        writer.Write(prompt);

        var response = Console.ReadLine();
        if (response == null)
        {
            return null;
        }

        var trimmed = response.Trim();
        return string.Equals(trimmed, "y", StringComparison.OrdinalIgnoreCase)
            || string.Equals(trimmed, "yes", StringComparison.OrdinalIgnoreCase);
    }

    private static void WritePreview(TextWriter writer, string heading, IReadOnlyList<string> tables)
    {
        writer.WriteLine(heading);
        if (tables.Count == 0)
        {
            writer.WriteLine("  none");
        }
        else
        {
            foreach (var table in tables)
            {
                writer.WriteLine($"  {table}");
            }
        }
    }
}
