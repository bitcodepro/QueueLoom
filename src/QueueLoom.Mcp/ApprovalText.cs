using System.Globalization;
using System.Text;

namespace QueueLoom.Mcp;

/// <summary>
/// Text for the approval dialog. Values there come from the model (request fields, the client's own name) or from
/// message producers (Message IDs of stored messages). Shown as is, a value with line breaks could add lines that look
/// like the dialog's own ("Environment: ...") and push the real ones away; a right-to-left override could reverse what
/// follows it. Each such value is one line, with every invisible or line-breaking character shown as an escape and
/// a literal backslash shown doubled, so a real line break and the written characters \n never look alike.
/// </summary>
internal static class ApprovalText
{
    public static string OneLine(string? value)
    {
        if (string.IsNullOrEmpty(value)) return value ?? string.Empty;
        StringBuilder? text = null;
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            var escape = character switch
            {
                '\\' => "\\\\",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                _ when char.IsControl(character) || char.GetUnicodeCategory(character) is UnicodeCategory.Format
                    or UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator
                    => "\\u" + ((int)character).ToString("X4", CultureInfo.InvariantCulture),
                _ => null
            };
            if (escape is null)
            {
                text?.Append(character);
                continue;
            }
            text ??= new StringBuilder(value, 0, index, value.Length + 16);
            text.Append(escape);
        }
        return text?.ToString() ?? value;
    }
}
