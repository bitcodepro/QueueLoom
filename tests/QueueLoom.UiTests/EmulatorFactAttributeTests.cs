namespace QueueLoom.UiTests;

// Broker UI tests used to "return" when their emulator was not configured, so a run without brokers reported them as
// passed. They now skip, and the reason names exactly what is missing.
public sealed class EmulatorFactAttributeTests
{
    private static Func<string, string?> Env(params (string Name, string? Value)[] values) =>
        name => values.FirstOrDefault(value => value.Name == name).Value;

    [Fact]
    public void AMissingVariableSkipsAndIsNamed() =>
        Assert.Equal("Set QUEUELOOM_KAFKA to run this test against a local emulator.",
            EmulatorFactAttribute.SkipReason([Emulators.Kafka], Env()));

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ABlankVariableCountsAsMissing(string value) =>
        Assert.NotNull(EmulatorFactAttribute.SkipReason([Emulators.Kafka], Env((Emulators.Kafka, value))));

    [Fact]
    public void ASetVariableRuns() =>
        Assert.Null(EmulatorFactAttribute.SkipReason([Emulators.Kafka], Env((Emulators.Kafka, "localhost:9092"))));

    [Fact]
    public void EveryVariableMustBeSetAndOnlyTheMissingOnesAreNamed()
    {
        string[] both = [Emulators.LocalStack, Emulators.PubSub];

        Assert.Null(EmulatorFactAttribute.SkipReason(both, Env((Emulators.LocalStack, "http://x"), (Emulators.PubSub, "y:1"))));
        Assert.Equal("Set QUEUELOOM_PUBSUB_EMULATOR to run this test against a local emulator.",
            EmulatorFactAttribute.SkipReason(both, Env((Emulators.LocalStack, "http://x"))));
        Assert.Equal("Set QUEUELOOM_LOCALSTACK_URL and QUEUELOOM_PUBSUB_EMULATOR to run this test against a local emulator.",
            EmulatorFactAttribute.SkipReason(both, Env()));
    }

    [Fact]
    public void NoVariableIsAMistake() =>
        Assert.Throws<ArgumentException>(() => EmulatorFactAttribute.SkipReason([], Env()));
}
