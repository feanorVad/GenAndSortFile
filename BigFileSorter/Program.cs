using System.Buffers;
using System.Collections.Concurrent;
using System.Text;

class Program
{
    const int MaxLinesInMemory = 4_000_000;

    readonly record struct LineSortKey(string TextPart, int NumberPart) : IComparable<LineSortKey>
    {
        public int CompareTo(LineSortKey other)
        {
            int cmp = string.Compare(TextPart, other.TextPart, StringComparison.Ordinal);
            return cmp != 0 ? cmp : NumberPart.CompareTo(other.NumberPart);
        }
    }

    static async Task Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.WriteLine("Usage: Sorter <input_file> <output_file>");
            return;
        }

        string inputPath = args[0];
        string outputPath = args[1];

        Console.WriteLine($"Sorting {inputPath} into {outputPath}...");

        string tempDir = Path.Combine(Path.GetTempPath(), "SorterTemp_" + Guid.NewGuid());
        Directory.CreateDirectory(tempDir);

        var chunkFiles = await SplitAndSortChunksParallel(inputPath, tempDir);
        await MergeSortedChunks(chunkFiles, outputPath);

        Directory.Delete(tempDir, true);
        Console.WriteLine("Done Sorting!");
    }

    static async Task<List<string>> SplitAndSortChunksParallel(string inputPath, string tempDir)
    {
        var chunkFiles = new ConcurrentBag<string>();
        var tasks = new List<Task>();

        using var reader = new StreamReader(inputPath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4 * 1024 * 1024);
        int chunkIndex = 0;

        while (!reader.EndOfStream)
        {
            var lines = new List<(string Line, LineSortKey Key)>(MaxLinesInMemory);
            while (lines.Count < MaxLinesInMemory && !reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(line)) continue;
                ParseLine(line, out int number, out string text);
                lines.Add((line, new LineSortKey(text, number)));
            }

            int currentChunkIndex = chunkIndex++;
            var linesCopy = new List<(string Line, LineSortKey Key)>(lines);

            tasks.Add(Task.Run(async () =>
            {
                linesCopy.Sort((a, b) => a.Key.CompareTo(b.Key));

                string chunkPath = Path.Combine(tempDir, $"chunk_{currentChunkIndex:D4}.txt");
                using var writer = new StreamWriter(chunkPath, false, Encoding.UTF8, bufferSize: 64 * 1024 * 1024);
                foreach (var (line, _) in linesCopy)
                    await writer.WriteLineAsync(line);

                chunkFiles.Add(chunkPath);
            }));
        }

        await Task.WhenAll(tasks);
        return chunkFiles.OrderBy(x => x).ToList();
    }

    static async Task MergeSortedChunks(List<string> chunkFiles, string outputPath)
    {
        const int groupSize = 4;
        var intermediateFiles = new List<string>();

        var groups = chunkFiles
            .Select((file, index) => new { file, index })
            .GroupBy(x => x.index / groupSize)
            .Select(g => g.Select(x => x.file).ToList())
            .ToList();       

        await Task.WhenAll(groups.Select(async (group, i) =>
        {
            string tempOut = Path.Combine(Path.GetTempPath(), $"merged_group_{i}.txt");
            await MergeGroup(group, tempOut);
            lock (intermediateFiles)
                intermediateFiles.Add(tempOut);
        }));
       
        await MergeGroup(intermediateFiles, outputPath);

        foreach (var f in intermediateFiles)
            File.Delete(f);
    }

    static async Task MergeGroup(List<string> chunkFiles, string outputPath)
    {
        var readers = chunkFiles
            .Select(path => new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024 * 1024))
            .ToList();

        var pq = new PriorityQueue<(string Line, int Index), LineSortKey>();

        for (int i = 0; i < readers.Count; i++)
        {
            var line = await readers[i].ReadLineAsync();
            if (line != null)
            {
                ParseLine(line, out int num, out string text);
                pq.Enqueue((line, i), new LineSortKey(text, num));
            }
        }

        using var writer = new StreamWriter(outputPath, false, Encoding.UTF8, bufferSize: 64 * 1024 * 1024);

        while (pq.Count > 0)
        {
            var (line, idx) = pq.Dequeue();
            await writer.WriteLineAsync(line);

            var nextLine = await readers[idx].ReadLineAsync();
            if (nextLine != null)
            {
                ParseLine(nextLine, out int num, out string text);
                pq.Enqueue((nextLine, idx), new LineSortKey(text, num));
            }
        }

        foreach (var r in readers)
            r.Dispose();
    }

    static void ParseLine(string line, out int number, out string text)
    {
        int dotIndex = line.IndexOf('.');
        if (dotIndex < 0 || dotIndex >= line.Length - 1)
            throw new FormatException($"Line in unexpected format: '{line}'");

        number = int.Parse(line.AsSpan(0, dotIndex));
        text = line.Substring(dotIndex + 1);
    }
}
