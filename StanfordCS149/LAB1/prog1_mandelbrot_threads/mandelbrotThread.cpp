#include <stdio.h>
#include <thread>
#include <atomic>
#include "CycleTimer.h"

typedef struct
{
    float x0, x1;
    float y0, y1;
    unsigned int width;
    unsigned int height;
    int maxIterations;
    int *output;
    int threadId;
    int numThreads;
    std::atomic<int> *nextRow; // Added for dynamic work distribution
} WorkerArgs;

extern void mandelbrotSerial(
    float x0, float y0, float x1, float y1,
    int width, int height,
    int startRow, int totalRows,
    int maxIterations,
    int output[]);

void workerThreadStart(WorkerArgs *const args)
{
    const int CHUNK_SIZE = 16;
    std::atomic<int> &nextRowIndex = *args->nextRow;

    while (true)
    {
        uint startRow = nextRowIndex.fetch_add(CHUNK_SIZE, std::memory_order_relaxed);
        if (startRow >= args->height / 2)
        {
            break;
        }

        int rowsToProcess = std::min(CHUNK_SIZE, static_cast<int>(args->height / 2 - startRow));

        mandelbrotSerial(
            args->x0, args->y0,
            args->x1, args->y1,
            args->width, args->height,
            startRow, rowsToProcess,
            args->maxIterations,
            args->output);
    }
}

void mandelbrotThread(
    int numThreads,
    float x0, float y0, float x1, float y1,
    int width, int height,
    int maxIterations, int output[])
{
    static constexpr int MAX_THREADS = 64;

    if (numThreads > MAX_THREADS)
    {
        fprintf(stderr, "Error: Max allowed threads is %d\n", MAX_THREADS);
        exit(1);
    }

    // Creates thread objects that do not yet represent a thread.
    std::thread workers[MAX_THREADS];
    WorkerArgs args[MAX_THREADS];
    std::atomic<int> nextRow(0); // Shared counter for work distribution

    for (int i = 0; i < numThreads; i++)
    {
        args[i].x0 = x0;
        args[i].y0 = y0;
        args[i].x1 = x1;
        args[i].y1 = y1;
        args[i].width = width;
        args[i].height = height;
        args[i].maxIterations = maxIterations;
        args[i].numThreads = numThreads;
        args[i].output = output;
        args[i].threadId = i;
        args[i].nextRow = &nextRow; // Share the atomic counter
    }

    // Spawn all worker threads (including the main thread)
    for (int i = 1; i < numThreads; i++)
    {
        workers[i] = std::thread(workerThreadStart, &args[i]);
    }

    // Main thread also works
    workerThreadStart(&args[0]);

    // Join worker threads
    for (int i = 1; i < numThreads; i++)
    {
        workers[i].join();
    }
}