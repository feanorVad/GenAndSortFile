using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

class Program
{
    static readonly string[] SampleStrings = new[]
    {
        "Apple", "Apple two", "Banana is yellow", "Cherry is the best", "Something something something",
        "Orange is round", "Watermelon is large", "Grapes are sweet", "Pineapple has a crown",
        "Mango is tropical", "Blueberries are tiny", "Kiwi has seeds", "Lemon is sour",
        "Lime is green", "Strawberries are red", "Peach is fuzzy", "Plum is purple",
        "Pear is juicy", "Raspberry is delicate", "Blackberry is wild", "Coconut has water",
        "Avocado is creamy", "Papaya is exotic", "Fig is ancient", "Date is sweet", "Lychee is fragrant",
        "Guava is underrated", "Pomegranate is full of seeds", "Dragonfruit looks weird"
};


    static async Task Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.WriteLine("Usage: UltraFastGenerator <output_file> <target_size_in_MB>");
            return;
        }

        string outputPath = args[0];
        if (!long.TryParse(args[1], out long targetSizeMb) || targetSizeMb <= 0)
        {
            Console.WriteLine("Invalid size. Provide a positive integer for size in MB.");
            return;
        }

        long targetSizeBytes = targetSizeMb * 1024L * 1024L;
        long totalBytesWritten = 0;
        var queue = new BlockingCollection<byte[]>(boundedCapacity: 200_000);
        var cts = new CancellationTokenSource();
        var token = cts.Token;
        var sw = Stopwatch.StartNew();

        var writerTask = Task.Run(() =>
        {
            using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024 * 1024);
            foreach (var lineBytes in queue.GetConsumingEnumerable(token))
            {
                if (token.IsCancellationRequested) break;

                fs.Write(lineBytes, 0, lineBytes.Length);

                Interlocked.Add(ref totalBytesWritten, lineBytes.Length);
                if (Interlocked.Read(ref totalBytesWritten) >= targetSizeBytes)
                {
                    cts.Cancel();
                    break;
                }
            }
        });
       
        int threadCount = Environment.ProcessorCount + 2;
        Parallel.For(0, threadCount, _ =>
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            var localBuffer = new byte[128];

            while (!token.IsCancellationRequested)
            {
                int number = rnd.Next(1, 100_000);
                string text = SampleStrings[rnd.Next(SampleStrings.Length)];
                string line = $"{number}.{text}\n";

                int byteCount = Encoding.UTF8.GetByteCount(line);
                byte[] buffer = new byte[byteCount];
                Encoding.UTF8.GetBytes(line, 0, line.Length, buffer, 0);

                try
                {
                    queue.Add(buffer, token);
                }
                catch
                {
                    break;
                }

                if (Interlocked.Read(ref totalBytesWritten) >= targetSizeBytes)
                {
                    cts.Cancel();
                    break;
                }
            }
        });
       
        queue.CompleteAdding();
        await writerTask;

        sw.Stop();
        double mb = totalBytesWritten / (1024.0 * 1024.0);
        Console.WriteLine($"Done. File size: {mb:F2} MB");
    }
}
