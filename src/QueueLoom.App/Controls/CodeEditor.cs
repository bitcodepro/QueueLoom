using System.Xml;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Data;
using Avalonia.Media;
using Avalonia.Styling;
using AvaloniaEdit;
using AvaloniaEdit.Highlighting;
using AvaloniaEdit.Highlighting.Xshd;

namespace QueueLoom.App.Controls;

/// <summary>
/// A bindable code editor with line numbers and theme-aware JSON highlighting.
/// Wraps AvaloniaEdit's <see cref="TextEditor"/>, whose text is not a styled property.
/// </summary>
public sealed class CodeEditor : UserControl
{
    public static readonly StyledProperty<string?> TextProperty =
        AvaloniaProperty.Register<CodeEditor, string?>(
            nameof(Text),
            defaultBindingMode: BindingMode.TwoWay);

    public static readonly StyledProperty<bool> IsReadOnlyProperty =
        AvaloniaProperty.Register<CodeEditor, bool>(nameof(IsReadOnly));

    public static readonly StyledProperty<bool> HighlightJsonProperty =
        AvaloniaProperty.Register<CodeEditor, bool>(nameof(HighlightJson), true);

    public static readonly StyledProperty<bool> WordWrapProperty =
        AvaloniaProperty.Register<CodeEditor, bool>(nameof(WordWrap), true);

    /// <summary>1-based line to reveal and select, e.g. the location of a JSON parse error.</summary>
    public static readonly StyledProperty<int?> ErrorLineProperty =
        AvaloniaProperty.Register<CodeEditor, int?>(nameof(ErrorLine));

    private static readonly Dictionary<ThemeVariant, IHighlightingDefinition> Definitions = [];
    private readonly TextEditor _editor;
    private bool _syncing;

    public CodeEditor()
    {
        _editor = new TextEditor
        {
            ShowLineNumbers = true,
            FontSize = 12.5,
            WordWrap = true,
            HorizontalScrollBarVisibility = Avalonia.Controls.Primitives.ScrollBarVisibility.Disabled,
            VerticalScrollBarVisibility = Avalonia.Controls.Primitives.ScrollBarVisibility.Auto,
            Padding = new Thickness(6, 8, 6, 8)
        };
        _editor.Options.EnableHyperlinks = false;
        _editor.Options.EnableEmailHyperlinks = false;
        _editor.Options.ConvertTabsToSpaces = true;
        _editor.Options.IndentationSize = 2;
        _editor.Bind(TextEditor.FontFamilyProperty, this.GetResourceObservable("MonoFontFamily"));
        _editor.Bind(TextEditor.ForegroundProperty, this.GetResourceObservable("TextPrimaryBrush"));
        _editor.Bind(TextEditor.LineNumbersForegroundProperty, this.GetResourceObservable("TextSecondaryBrush"));
        _editor.Background = Brushes.Transparent;
        _editor.TextChanged += OnEditorTextChanged;
        Content = _editor;
        // Caret updates inside an unfocused editor must not scroll the surrounding page.
        AddHandler(RequestBringIntoViewEvent, (_, args) =>
        {
            if (!_editor.IsKeyboardFocusWithin)
            {
                args.Handled = true;
            }
        }, Avalonia.Interactivity.RoutingStrategies.Bubble);
        ActualThemeVariantChanged += (_, _) => ApplyHighlighting();
    }

    public string? Text
    {
        get => GetValue(TextProperty);
        set => SetValue(TextProperty, value);
    }

    public bool IsReadOnly
    {
        get => GetValue(IsReadOnlyProperty);
        set => SetValue(IsReadOnlyProperty, value);
    }

    public bool HighlightJson
    {
        get => GetValue(HighlightJsonProperty);
        set => SetValue(HighlightJsonProperty, value);
    }

    public bool WordWrap
    {
        get => GetValue(WordWrapProperty);
        set => SetValue(WordWrapProperty, value);
    }

    public int? ErrorLine
    {
        get => GetValue(ErrorLineProperty);
        set => SetValue(ErrorLineProperty, value);
    }

    /// <summary>The underlying editor, exposed for focus handling and tests.</summary>
    public TextEditor Editor => _editor;

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == TextProperty && !_syncing)
        {
            var text = change.GetNewValue<string?>() ?? string.Empty;
            if (!string.Equals(_editor.Text, text, StringComparison.Ordinal))
            {
                _syncing = true;
                try
                {
                    _editor.Text = text;
                }
                finally
                {
                    _syncing = false;
                }
            }
        }
        else if (change.Property == IsReadOnlyProperty)
        {
            _editor.IsReadOnly = change.GetNewValue<bool>();
        }
        else if (change.Property == WordWrapProperty)
        {
            var wrap = change.GetNewValue<bool>();
            _editor.WordWrap = wrap;
            _editor.HorizontalScrollBarVisibility = wrap
                ? Avalonia.Controls.Primitives.ScrollBarVisibility.Disabled
                : Avalonia.Controls.Primitives.ScrollBarVisibility.Auto;
        }
        else if (change.Property == HighlightJsonProperty)
        {
            ApplyHighlighting();
        }
        else if (change.Property == ErrorLineProperty)
        {
            RevealLine(change.GetNewValue<int?>());
        }
    }

    protected override void OnAttachedToVisualTree(VisualTreeAttachmentEventArgs e)
    {
        base.OnAttachedToVisualTree(e);
        ApplyHighlighting();
    }

    private void OnEditorTextChanged(object? sender, EventArgs args)
    {
        if (_syncing)
        {
            return;
        }

        _syncing = true;
        try
        {
            SetCurrentValue(TextProperty, _editor.Text);
        }
        finally
        {
            _syncing = false;
        }
    }

    private void RevealLine(int? line)
    {
        if (line is not { } number || number < 1 || _editor.Document is null || number > _editor.Document.LineCount)
        {
            return;
        }

        var documentLine = _editor.Document.GetLineByNumber(number);
        _editor.ScrollToLine(number);
        _editor.Select(documentLine.Offset, documentLine.Length);
    }

    private void ApplyHighlighting()
    {
        if (!HighlightJson)
        {
            _editor.SyntaxHighlighting = null;
            return;
        }

        var variant = ActualThemeVariant == ThemeVariant.Light ? ThemeVariant.Light : ThemeVariant.Dark;
        if (!Definitions.TryGetValue(variant, out var definition))
        {
            definition = LoadJsonDefinition(variant == ThemeVariant.Light
                ? new JsonPalette("#0A5FA8", "#157A4C", "#9A5B00", "#6B3FB5", "#6A7788")
                : new JsonPalette("#7CC4FF", "#8EE6B2", "#FFC47A", "#C4A7FF", "#7C8FA8"));
            Definitions[variant] = definition;
        }

        _editor.SyntaxHighlighting = definition;
    }

    private static IHighlightingDefinition LoadJsonDefinition(JsonPalette palette)
    {
        var xshd = $$"""
            <SyntaxDefinition name="QueueLoomJson" xmlns="http://icsharpcode.net/sharpdevelop/syntaxdefinition/2008">
              <Color name="Key" foreground="{{palette.Key}}" />
              <Color name="String" foreground="{{palette.String}}" />
              <Color name="Number" foreground="{{palette.Number}}" />
              <Color name="Literal" foreground="{{palette.Literal}}" fontWeight="bold" />
              <Color name="Punctuation" foreground="{{palette.Punctuation}}" />
              <RuleSet>
                <Rule color="Key">"(?:[^"\\]|\\.)*"(?=\s*:)</Rule>
                <Span color="String" multiline="false">
                  <Begin>"</Begin>
                  <End>"</End>
                  <RuleSet>
                    <Span begin="\\" end="." />
                  </RuleSet>
                </Span>
                <Keywords color="Literal">
                  <Word>true</Word>
                  <Word>false</Word>
                  <Word>null</Word>
                </Keywords>
                <Rule color="Number">-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b</Rule>
                <Rule color="Punctuation">[{}\[\]:,]</Rule>
              </RuleSet>
            </SyntaxDefinition>
            """;
        using var reader = XmlReader.Create(new StringReader(xshd));
        return HighlightingLoader.Load(reader, HighlightingManager.Instance);
    }

    private sealed record JsonPalette(string Key, string String, string Number, string Literal, string Punctuation);
}
