using System.Text.Json;

return await SdkCustody.RunAsync(args, async arguments =>
{
    if (arguments.Length != 5 && !(arguments.Length == 6 && arguments[5] == "subscription-get"))
    {
        Console.Error.WriteLine(
            "usage: <declared-current|previous> <absolute-fixture-root> <absolute-approved-assets.json> <expected-entry-sha256> <expected-approval-sha256> [subscription-get]");
        return 2;
    }

    using var inputs = new FixtureInputs(arguments);
    if (arguments.Length == 6)
    {
        await SubscriptionGetControls.RunAsync(inputs);
    }
    else
    {
        await AdministrationControls.RunAsync(inputs);
    }
    inputs.VerifyUnchanged();
    Console.WriteLine("SWITCHYARD_ATOM_COMPLETE " + JsonSerializer.Serialize(new
    {
        schema = 1,
        nonce = Environment.GetEnvironmentVariable("SWITCHYARD_SDK_NONCE"),
        selector = inputs.Selector,
        transport = "in-memory-no-network",
        response_evidence = arguments.Length == 6 ? "rust-test-https-captures-in-memory-sdk-controls" : "synthetic-fixtures",
        fixture_inputs = inputs.Fingerprints,
    }));
    return 0;
});
