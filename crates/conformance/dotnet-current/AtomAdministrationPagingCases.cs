using Azure;
using Azure.Messaging.ServiceBus.Administration;

internal static class AtomAdministrationPagingCases
{
    internal static async Task RunAsync(
        ServiceBusAdministrationClient named,
        ServiceBusAdministrationClient connection,
        CancellationToken token)
    {
        await RequirePagesAsync(named, 0, new[] { 0 }, token);
        await RequirePagesAsync(connection, 0, new[] { 0 }, token);
        for (int index = 0; index < 100; index++)
        {
            Response<QueueProperties> created = await named.CreateQueueAsync(Name(index), token);
            AtomAdministrationCases.RequireStatus(created.GetRawResponse(), 201);
            AtomAdministrationCases.RequireDefault(created.Value, Name(index));
        }
        await RequirePagesAsync(named, 100, new[] { 100, 0 }, token);
        await RequirePagesAsync(connection, 100, new[] { 100, 0 }, token);
        Response<QueueProperties> last = await connection.CreateQueueAsync(Name(100), token);
        AtomAdministrationCases.RequireStatus(last.GetRawResponse(), 201);
        AtomAdministrationCases.RequireDefault(last.Value, Name(100));
        await RequirePagesAsync(named, 101, new[] { 100, 1 }, token);
        await RequirePagesAsync(connection, 101, new[] { 100, 1 }, token);
        for (int index = 0; index < 101; index++)
        {
            AtomAdministrationCases.RequireStatus(await named.DeleteQueueAsync(Name(index), token), 200);
        }
        await RequirePagesAsync(named, 0, new[] { 0 }, token);
        await RequirePagesAsync(connection, 0, new[] { 0 }, token);
    }

    internal static async Task RequireEmptyAsync(
        ServiceBusAdministrationClient client, CancellationToken token)
    {
        await RequirePagesAsync(client, 0, new[] { 0 }, token);
    }

    private static string Name(int index) => $"sdk-atom-page-{index:D3}";

    private static async Task RequirePagesAsync(
        ServiceBusAdministrationClient client, int count,
        IReadOnlyList<int> expectedPages, CancellationToken token)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var pageSizes = new List<int>();
        await foreach (Page<QueueProperties> page in client.GetQueuesAsync(token).AsPages())
        {
            AtomAdministrationCases.RequireStatus(page.GetRawResponse(), 200);
            pageSizes.Add(page.Values.Count);
            AtomAdministrationCases.Require(pageSizes.Count <= 3, "Page work bound.");
            foreach (QueueProperties queue in page.Values)
            {
                AtomAdministrationCases.RequireDefault(queue, queue.Name);
                AtomAdministrationCases.Require(names.Add(queue.Name), "Duplicate queue in feed.");
            }
        }
        AtomAdministrationCases.Require(pageSizes.SequenceEqual(expectedPages), "Complete SDK page sizes.");
        AtomAdministrationCases.Require(names.Count == count, "Complete SDK queue count.");
        var expectedNames = Enumerable.Range(0, count).Select(Name).ToHashSet(StringComparer.Ordinal);
        AtomAdministrationCases.Require(names.SetEquals(expectedNames), "Exact ordinal SDK queue names.");
    }
}
