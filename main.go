package main

import (
        "context"
        "encoding/json"
        "flag"
        "fmt"
        "log"
        "os"
        "runtime"
        "strings"
        "sync"
        "sync/atomic"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"
)

// ANSI Color codes for terminal output
const (
        ColorReset  = "\033[0m"
        ColorRed    = "\033[31m"
        ColorGreen  = "\033[32m"
        ColorYellow = "\033[33m"        ColorBlue   = "\033[34m"
        ColorPurple = "\033[35m"
        ColorCyan   = "\033[36m"
        ColorWhite  = "\033[37m"

        // Bold colors
        ColorBoldRed    = "\033[1;31m"
        ColorBoldGreen  = "\033[1;32m"
        ColorBoldYellow = "\033[1;33m"
        ColorBoldBlue   = "\033[1;34m"
        ColorBoldPurple = "\033[1;35m"
        ColorBoldCyan   = "\033[1;36m"

        // Background colors
        BgRed    = "\033[41m"
        BgGreen  = "\033[42m"
        BgYellow = "\033[43m"
)

// ComparisonStrategy defines the approach to use
type ComparisonStrategy string

const (
        AutoStrategy           ComparisonStrategy = "auto"
        TraditionalStrategy    ComparisonStrategy = "traditional"
        TwoLevelHierarchy     ComparisonStrategy = "two-level"
        ThreeLevelHierarchy   ComparisonStrategy = "three-level"
        FourLevelHierarchy    ComparisonStrategy = "four-level"
)

// Config holds the configuration for table comparison
type Config struct {
        SourceURL      string
        TargetURL      string
        TableName      string
        SchemaName     string
        WorkerCount    int
        BatchSize      int
        MaxConnections int
        OutputFile     string
        SQLFixFile     string
        Verbose        bool
        NoColor        bool
        Strategy       ComparisonStrategy

        // Hierarchical configuration
        CoarsePartitions      int     // Level 1 partitions
        FinePartitionSize     int64   // Level 2 partition size
        MicroPartitionSize    int64   // Level 3 partition size
        RowLevelThreshold     int64   // When to switch to row-level
        DifferenceThreshold   float64 // Threshold for strategy adaptation
}

// TableStats holds table statistics
type TableStats struct {
        RowCount    int64
        MinID       int64
        MaxID       int64
        PrimaryKey  string
        ColumnNames []string
}

// Difference types
type DifferenceType string

const (
        Missing  DifferenceType = "missing"
        Extra    DifferenceType = "extra"
        Modified DifferenceType = "modified"
)

// Difference represents a single difference found
type Difference struct {
        Type       DifferenceType
        ID         int64
        SourceData map[string]interface{}
        TargetData map[string]interface{}
        Changes    map[string]ChangeDetail
}

// ChangeDetail represents a field-level change
type ChangeDetail struct {
        OldValue interface{} `json:"old_value"`
        NewValue interface{} `json:"new_value"`
}

// HierarchicalPartition represents a partition at any level
type HierarchicalPartition struct {
        Level   int
        StartID int64
        EndID   int64
        Hash    string
        Parent  *HierarchicalPartition
}

// Partition represents a work unit for a worker (backward compatibility)
type Partition struct {
        StartID int64
        EndID   int64
}

// ComparisonResult holds the final comparison results
type ComparisonResult struct {
        Summary struct {
                SourceDB         string             `json:"source_db"`
                TargetDB         string             `json:"target_db"`
                Table            string             `json:"table"`
                Strategy         ComparisonStrategy `json:"strategy"`
                TotalSourceRows  int64              `json:"total_source_rows"`
                TotalTargetRows  int64              `json:"total_target_rows"`
                RowsChecked      int64              `json:"rows_checked"`
                ExecutionTime    time.Duration      `json:"execution_time"`
                MissingCount     int                `json:"missing_count"`
                ExtraCount       int                `json:"extra_count"`
                ModifiedCount    int                `json:"modified_count"`
                MatchPercentage  float64            `json:"match_percentage"`
                PartitionsSkipped int               `json:"partitions_skipped"`
                PartitionsProcessed int             `json:"partitions_processed"`
        } `json:"summary"`
        MissingRows  []Difference `json:"missing_rows"`
        ExtraRows    []Difference `json:"extra_rows"`
        ModifiedRows []Difference `json:"modified_rows"`
}

// TableComparator is the main struct for comparing tables
type TableComparator struct {
        config       *Config
        sourcePool   *pgxpool.Pool
        targetPool   *pgxpool.Pool
        sourceStats  *TableStats
        targetStats  *TableStats
        differences  *sync.Map
        rowsChecked  int64
        partitionsSkipped   int64
        partitionsProcessed int64
        progressChan chan string
        wg           sync.WaitGroup
}

// Helper function to colorize text
func (tc *TableComparator) colorize(text string, color string) string {
        if tc.config.NoColor {
                return text
        }
        return color + text + ColorReset
}

func main() {
        config := parseFlags()

        comparator, err := NewTableComparator(config)
        if err != nil {
                log.Fatalf("Failed to initialize comparator: %v", err)
        }
        defer comparator.Close()

        startTime := time.Now()
        result, err := comparator.Compare()
        if err != nil {
                log.Fatalf("Comparison failed: %v", err)
        }

        result.Summary.ExecutionTime = time.Since(startTime)

        // Output results
        if err := comparator.outputResults(result); err != nil {
                log.Fatalf("Failed to output results: %v", err)
        }

        // Summary with colors
        fmt.Printf("\n%s\n", comparator.colorize("✓ Comparison completed in "+result.Summary.ExecutionTime.String(), ColorBoldGreen))
        fmt.Printf("Strategy used: %s%s%s\n", ColorBoldCyan, result.Summary.Strategy, ColorReset)

        totalDiffs := result.Summary.MissingCount + result.Summary.ExtraCount + result.Summary.ModifiedCount
        if totalDiffs == 0 {
                fmt.Printf("%s\n", comparator.colorize("🎉 Tables are identical!", ColorBoldGreen))
        } else {
                fmt.Printf("%s: %s%d%s (%s%d%s missing, %s%d%s extra, %s%d%s modified)\n",
                        comparator.colorize("Total differences found", ColorBoldYellow),
                        ColorBoldRed, totalDiffs, ColorReset,
                        ColorRed, result.Summary.MissingCount, ColorReset,
                        ColorYellow, result.Summary.ExtraCount, ColorReset,
                        ColorBlue, result.Summary.ModifiedCount, ColorReset)
        }

        if result.Summary.PartitionsSkipped > 0 {
                fmt.Printf("Efficiency: %s%d%s partitions skipped, %s%d%s processed\n",
                        ColorBoldGreen, result.Summary.PartitionsSkipped, ColorReset,
                        ColorBoldYellow, result.Summary.PartitionsProcessed, ColorReset)
        }
}

func parseFlags() *Config {
        config := &Config{}
        var strategyStr string

        flag.StringVar(&config.SourceURL, "source", "", "Source database URL (postgres://...)")
        flag.StringVar(&config.TargetURL, "target", "", "Target database URL (postgres://...)")
        flag.StringVar(&config.TableName, "table", "", "Table name to compare")
        flag.StringVar(&config.SchemaName, "schema", "public", "Schema name")
        flag.IntVar(&config.WorkerCount, "workers", runtime.NumCPU()*4, "Number of parallel workers")
        flag.IntVar(&config.BatchSize, "batch", 10000, "Batch size for fetching rows")
        flag.IntVar(&config.MaxConnections, "connections", 25, "Max connections per pool")
        flag.StringVar(&config.OutputFile, "output", "", "Output file for results (JSON)")
        flag.StringVar(&config.SQLFixFile, "sql-fix", "", "Generate SQL fix script file")
        flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output")
        flag.BoolVar(&config.NoColor, "no-color", false, "Disable colored output")

        // Strategy selection
        flag.StringVar(&strategyStr, "strategy", "auto",
                "Comparison strategy: auto, traditional, two-level, three-level, four-level")

        // Hierarchical parameters
        flag.IntVar(&config.CoarsePartitions, "coarse-partitions", 0, "Number of coarse partitions (auto if 0)")
        flag.Int64Var(&config.FinePartitionSize, "fine-partition-size", 0, "Fine partition size (auto if 0)")
        flag.Int64Var(&config.MicroPartitionSize, "micro-partition-size", 0, "Micro partition size (auto if 0)")
        flag.Int64Var(&config.RowLevelThreshold, "row-threshold", 0, "Row-level threshold (auto if 0)")
        flag.Float64Var(&config.DifferenceThreshold, "diff-threshold", 0.25, "Difference threshold for strategy adaptation")

        flag.Usage = func() {
                fmt.Printf("Database Table Comparator with Smart Hierarchical Analysis\n\n")
                fmt.Printf("Usage: %s [OPTIONS]\n\n", os.Args[0])
                fmt.Printf("Required:\n")
                fmt.Printf("  -source     Source database URL (postgres://user:pass@host:port/db)\n")
                fmt.Printf("  -target     Target database URL (postgres://user:pass@host:port/db)\n")
                fmt.Printf("  -table      Table name to compare\n\n")
                fmt.Printf("Basic Options:\n")
                fmt.Printf("  -schema     Schema name (default: public)\n")
                fmt.Printf("  -verbose    Enable detailed logging\n")
                fmt.Printf("  -no-color   Disable colored output\n\n")
                fmt.Printf("Output Options:\n")
                fmt.Printf("  -output     JSON output file path\n")
                fmt.Printf("  -sql-fix    SQL fix script file path\n\n")
                fmt.Printf("Performance Options:\n")
                fmt.Printf("  -workers    Number of parallel workers (default: CPU_cores * 4)\n")
                fmt.Printf("  -connections Max connections per pool (default: 25)\n")
                fmt.Printf("  -batch      Batch size for fetching rows (default: 10000)\n\n")
                fmt.Printf("Strategy Options:\n")
                fmt.Printf("  -strategy   Comparison strategy:\n")
                fmt.Printf("              auto        - Smart mode selection (default)\n")
                fmt.Printf("              traditional - Single-level row hashing\n")
                fmt.Printf("              two-level   - Coarse + fine partitioning\n")
                fmt.Printf("              three-level - Coarse + fine + micro partitioning\n")
                fmt.Printf("              four-level  - Maximum hierarchical depth\n\n")
                fmt.Printf("Advanced Hierarchical Options:\n")
                fmt.Printf("  -coarse-partitions    Number of coarse partitions (auto if 0)\n")
                fmt.Printf("  -fine-partition-size  Fine partition size (auto if 0)\n")
                fmt.Printf("  -micro-partition-size Micro partition size (auto if 0)\n")
                fmt.Printf("  -row-threshold        Row-level threshold (auto if 0)\n")
                fmt.Printf("  -diff-threshold       Difference threshold for adaptation (default: 0.25)\n\n")
                fmt.Printf("Examples:\n")
                fmt.Printf("  # Auto mode (recommended)\n")
                fmt.Printf("  %s -source postgres://... -target postgres://... -table users\n\n", os.Args[0])
                fmt.Printf("  # Force traditional mode for small tables\n")
                fmt.Printf("  %s -source postgres://... -target postgres://... -table config -strategy traditional\n\n", os.Args[0])
                fmt.Printf("  # Large table with SQL fix generation\n")
                fmt.Printf("  %s -source postgres://... -target postgres://... -table transactions -sql-fix fixes.sql -verbose\n\n", os.Args[0])
                fmt.Printf("Strategy Selection Guidelines:\n")
                fmt.Printf("  < 500K rows     : traditional (fastest)\n")
                fmt.Printf("  500K - 10M rows : two-level (2-5x speedup)\n")
                fmt.Printf("  10M - 100M rows : three-level (5-15x speedup)\n")
                fmt.Printf("  > 100M rows     : four-level (10-50x speedup)\n")
        }

        flag.Parse()

        if config.SourceURL == "" || config.TargetURL == "" || config.TableName == "" {
                flag.Usage()
                log.Fatal("\nError: Source URL, target URL, and table name are required")
        }

        // Parse strategy
        config.Strategy = ComparisonStrategy(strings.ToLower(strategyStr))
        validStrategies := map[ComparisonStrategy]bool{
                AutoStrategy:        true,
                TraditionalStrategy: true,
                TwoLevelHierarchy:   true,
                ThreeLevelHierarchy: true,
                FourLevelHierarchy:  true,
        }

        if !validStrategies[config.Strategy] {
                log.Fatalf("Invalid strategy: %s. Valid options: auto, traditional, two-level, three-level, four-level", strategyStr)
        }

        return config
}

// Smart strategy selection based on estimated table size
func (tc *TableComparator) selectOptimalStrategy(estimatedRows int64) ComparisonStrategy {
        if tc.config.Strategy != AutoStrategy {
                return tc.config.Strategy
        }

        // Smart selection logic
        switch {
        case estimatedRows < 500_000:
                tc.log(fmt.Sprintf("Auto-selected %sTRADITIONAL%s strategy for %s%d%s estimated rows",
                        ColorBoldCyan, ColorReset, ColorBoldGreen, estimatedRows, ColorReset))
                return TraditionalStrategy

        case estimatedRows < 10_000_000:
                tc.log(fmt.Sprintf("Auto-selected %sTWO-LEVEL%s strategy for %s%d%s estimated rows",
                        ColorBoldCyan, ColorReset, ColorBoldGreen, estimatedRows, ColorReset))
                return TwoLevelHierarchy

        case estimatedRows < 100_000_000:
                tc.log(fmt.Sprintf("Auto-selected %sTHREE-LEVEL%s strategy for %s%d%s estimated rows",
                        ColorBoldCyan, ColorReset, ColorBoldGreen, estimatedRows, ColorReset))
                return ThreeLevelHierarchy

        default:
                tc.log(fmt.Sprintf("Auto-selected %sFOUR-LEVEL%s strategy for %s%d%s estimated rows",
                        ColorBoldCyan, ColorReset, ColorBoldGreen, estimatedRows, ColorReset))
                return FourLevelHierarchy
        }
}

// Configure hierarchical parameters based on strategy and table size
func (tc *TableComparator) configureHierarchicalParams(strategy ComparisonStrategy, estimatedRows int64) {
        // Use manual settings if provided
        if tc.config.CoarsePartitions > 0 && tc.config.FinePartitionSize > 0 &&
           tc.config.RowLevelThreshold > 0 {
                return
        }

        // Auto-configure based on strategy and table size
        switch strategy {
        case TwoLevelHierarchy:
                tc.config.CoarsePartitions = max(5, min(20, int(estimatedRows/500_000)))
                tc.config.FinePartitionSize = max64(50_000, estimatedRows/(int64(tc.config.CoarsePartitions)*10))
                tc.config.RowLevelThreshold = 25_000

        case ThreeLevelHierarchy:
                tc.config.CoarsePartitions = max(10, min(30, int(estimatedRows/1_000_000)))
                tc.config.FinePartitionSize = max64(100_000, estimatedRows/(int64(tc.config.CoarsePartitions)*20))
                tc.config.MicroPartitionSize = max64(10_000, tc.config.FinePartitionSize/20)
                tc.config.RowLevelThreshold = 5_000

        case FourLevelHierarchy:
                tc.config.CoarsePartitions = max(20, min(50, int(estimatedRows/5_000_000)))
                tc.config.FinePartitionSize = max64(500_000, estimatedRows/(int64(tc.config.CoarsePartitions)*40))
                tc.config.MicroPartitionSize = max64(50_000, tc.config.FinePartitionSize/30)
                tc.config.RowLevelThreshold = 2_000
        }

        tc.log(fmt.Sprintf("Configured: %s%d%s coarse partitions, %s%d%s fine size, %s%d%s row threshold",
                ColorBoldYellow, tc.config.CoarsePartitions, ColorReset,
                ColorBoldYellow, tc.config.FinePartitionSize, ColorReset,
                ColorBoldYellow, tc.config.RowLevelThreshold, ColorReset))
}


// Helper functions for int64
func max64(a, b int64) int64 {
        if a > b {
                return a
        }
        return b
}

func min64(a, b int64) int64 {
        if a < b {
                return a
        }
        return b
}

// Helper functions for int (keep existing for backward compatibility)
func max(a, b int) int {
        if a > b {
                return a
        }
        return b
}

func min(a, b int) int {
        if a < b {
                return a
        }
        return b
}


func NewTableComparator(config *Config) (*TableComparator, error) {
        ctx := context.Background()

        // Configure connection pools
        sourceConfig, err := pgxpool.ParseConfig(config.SourceURL)
        if err != nil {
                return nil, fmt.Errorf("invalid source URL: %w", err)
        }
        sourceConfig.MaxConns = int32(config.MaxConnections)
        sourceConfig.MinConns = 2

        targetConfig, err := pgxpool.ParseConfig(config.TargetURL)
        if err != nil {
                return nil, fmt.Errorf("invalid target URL: %w", err)
        }
        targetConfig.MaxConns = int32(config.MaxConnections)
        targetConfig.MinConns = 2

        // Create connection pools
        sourcePool, err := pgxpool.NewWithConfig(ctx, sourceConfig)
        if err != nil {
                return nil, fmt.Errorf("failed to create source pool: %w", err)
        }

        targetPool, err := pgxpool.NewWithConfig(ctx, targetConfig)
        if err != nil {
                sourcePool.Close()
                return nil, fmt.Errorf("failed to create target pool: %w", err)
        }

        return &TableComparator{
                config:       config,
                sourcePool:   sourcePool,
                targetPool:   targetPool,
                differences:  &sync.Map{},
                progressChan: make(chan string, 100),
        }, nil
}

func (tc *TableComparator) Close() {
        if tc.sourcePool != nil {
                tc.sourcePool.Close()
        }
        if tc.targetPool != nil {
                tc.targetPool.Close()
        }
        close(tc.progressChan)
}

func (tc *TableComparator) Compare() (*ComparisonResult, error) {
        ctx := context.Background()

        // Start progress reporter
        go tc.progressReporter()

        // Phase 1: Gather table statistics
        tc.log(tc.colorize("Phase 1: Gathering table statistics...", ColorBoldCyan))
        if err := tc.gatherStatistics(ctx); err != nil {
                return nil, fmt.Errorf("failed to gather statistics: %w", err)
        }

        // Phase 2: Smart strategy selection
        selectedStrategy := tc.selectOptimalStrategy(tc.sourceStats.RowCount)
        tc.configureHierarchicalParams(selectedStrategy, tc.sourceStats.RowCount)

        tc.log(fmt.Sprintf("%s: %s%d%s estimated rows (ID range: %s%d-%d%s)",
                tc.colorize("Source", ColorGreen),
                ColorBoldGreen, tc.sourceStats.RowCount, ColorReset,
                ColorCyan, tc.sourceStats.MinID, tc.sourceStats.MaxID, ColorReset))
        tc.log(fmt.Sprintf("%s: %s%d%s estimated rows (ID range: %s%d-%d%s)",
                tc.colorize("Target", ColorGreen),
                ColorBoldGreen, tc.targetStats.RowCount, ColorReset,
                ColorCyan, tc.targetStats.MinID, tc.targetStats.MaxID, ColorReset))
        tc.log(fmt.Sprintf("Primary key: %s%s%s", ColorBoldPurple, tc.sourceStats.PrimaryKey, ColorReset))

        // Phase 3: Execute comparison based on selected strategy
        var differences []Difference
        var err error

        switch selectedStrategy {
        case TraditionalStrategy:
                differences, err = tc.traditionalComparison(ctx)
        case TwoLevelHierarchy:
                differences, err = tc.twoLevelComparison(ctx)
        case ThreeLevelHierarchy:
                differences, err = tc.threeLevelComparison(ctx)
        case FourLevelHierarchy:
                differences, err = tc.fourLevelComparison(ctx)
        default:
                return nil, fmt.Errorf("unknown strategy: %s", selectedStrategy)
        }

        if err != nil {
                return nil, err
        }

        // Build result
        result := tc.buildResult(differences)
        result.Summary.Strategy = selectedStrategy
        result.Summary.PartitionsSkipped = int(atomic.LoadInt64(&tc.partitionsSkipped))
        result.Summary.PartitionsProcessed = int(atomic.LoadInt64(&tc.partitionsProcessed))

        return result, nil
}

// Traditional single-level comparison (existing logic)
func (tc *TableComparator) traditionalComparison(ctx context.Context) ([]Difference, error) {
        tc.log(tc.colorize("Using TRADITIONAL single-level comparison", ColorBoldPurple))

        // Create partitions
        partitions := tc.createPartitions()
        tc.log(fmt.Sprintf("Created %s%d%s partitions for %s%d%s workers",
                ColorBoldYellow, len(partitions), ColorReset,
                ColorBoldYellow, tc.config.WorkerCount, ColorReset))

        // Hash comparison
        tc.log(tc.colorize("Starting parallel hash comparison...", ColorBoldCyan))
        diffIDs := tc.performHashComparison(ctx, partitions)
        tc.log(fmt.Sprintf("Hash comparison complete. Found %s%d%s potential differences",
                ColorBoldYellow, len(diffIDs), ColorReset))

        // Fetch actual differences
        tc.log(tc.colorize("Fetching actual row data for differences...", ColorBoldCyan))
        differences := tc.fetchDifferences(ctx, diffIDs)

        atomic.AddInt64(&tc.partitionsProcessed, int64(len(partitions)))

        return differences, nil
}

// Two-level hierarchical comparison
func (tc *TableComparator) twoLevelComparison(ctx context.Context) ([]Difference, error) {
        tc.log(tc.colorize("Using TWO-LEVEL hierarchical comparison", ColorBoldPurple))

        // Level 1: Coarse partitions
        coarsePartitions := tc.createCoarsePartitions()
        tc.log(fmt.Sprintf("Level 1: Created %s%d%s coarse partitions",
                ColorBoldYellow, len(coarsePartitions), ColorReset))

        var allDifferences []Difference

        for i, coarsePartition := range coarsePartitions {
                tc.log(fmt.Sprintf("Level 1: Checking coarse partition %d (%d-%d)",
                        i+1, coarsePartition.StartID, coarsePartition.EndID))

                // Check if coarse partition has differences
                if tc.partitionHashesMatch(ctx, coarsePartition) {
                        tc.log(fmt.Sprintf("  %s✅ MATCH%s - Skipping %d rows",
                                ColorGreen, ColorReset, coarsePartition.EndID-coarsePartition.StartID+1))
                        atomic.AddInt64(&tc.partitionsSkipped, 1)
                        continue
                }

                tc.log(fmt.Sprintf("  %s❌ DIFFER%s - Deep diving into partition", ColorRed, ColorReset))
                atomic.AddInt64(&tc.partitionsProcessed, 1)

                // Level 2: Fine partitions within this coarse partition
                finePartitions := tc.createFinePartitions(coarsePartition)
                tc.log(fmt.Sprintf("  Level 2: Created %d fine partitions", len(finePartitions)))

                for _, finePartition := range finePartitions {
                        if tc.partitionHashesMatch(ctx, finePartition) {
                                atomic.AddInt64(&tc.partitionsSkipped, 1)
                                continue
                        }

                        atomic.AddInt64(&tc.partitionsProcessed, 1)

                        // Row-level comparison for this fine partition
                        partitionDiffs := tc.performRowLevelComparison(ctx, finePartition)
                        allDifferences = append(allDifferences, partitionDiffs...)
                }
        }

        return allDifferences, nil
}

// Three-level hierarchical comparison
func (tc *TableComparator) threeLevelComparison(ctx context.Context) ([]Difference, error) {
        tc.log(tc.colorize("Using THREE-LEVEL hierarchical comparison", ColorBoldPurple))

        // Level 1: Coarse partitions
        coarsePartitions := tc.createCoarsePartitions()
        tc.log(fmt.Sprintf("Level 1: Created %s%d%s coarse partitions",
                ColorBoldYellow, len(coarsePartitions), ColorReset))

        var allDifferences []Difference

        for i, coarsePartition := range coarsePartitions {
                tc.log(fmt.Sprintf("Level 1: Checking coarse partition %d", i+1))

                if tc.partitionHashesMatch(ctx, coarsePartition) {
                        tc.log(fmt.Sprintf("  %s✅ MATCH%s - Skipping coarse partition", ColorGreen, ColorReset))
                        atomic.AddInt64(&tc.partitionsSkipped, 1)
                        continue
                }

                tc.log(fmt.Sprintf("  %s❌ DIFFER%s - Checking fine partitions", ColorRed, ColorReset))
                atomic.AddInt64(&tc.partitionsProcessed, 1)

                // Level 2: Fine partitions
                finePartitions := tc.createFinePartitions(coarsePartition)

                for j, finePartition := range finePartitions {
                        if tc.partitionHashesMatch(ctx, finePartition) {
                                atomic.AddInt64(&tc.partitionsSkipped, 1)
                                continue
                        }

                        tc.log(fmt.Sprintf("    Level 2: Fine partition %d differs - checking micro partitions", j+1))
                        atomic.AddInt64(&tc.partitionsProcessed, 1)

                        // Level 3: Micro partitions
                        microPartitions := tc.createMicroPartitions(finePartition)

                        for _, microPartition := range microPartitions {
                                if tc.partitionHashesMatch(ctx, microPartition) {
                                        atomic.AddInt64(&tc.partitionsSkipped, 1)
                                        continue
                                }

                                atomic.AddInt64(&tc.partitionsProcessed, 1)

                                // Row-level comparison
                                partitionDiffs := tc.performRowLevelComparison(ctx, microPartition)
                                allDifferences = append(allDifferences, partitionDiffs...)
                        }
                }
        }

        return allDifferences, nil
}

// Four-level hierarchical comparison (maximum depth)
func (tc *TableComparator) fourLevelComparison(ctx context.Context) ([]Difference, error) {
        tc.log(tc.colorize("Using FOUR-LEVEL hierarchical comparison", ColorBoldPurple))

        coarsePartitions := tc.createCoarsePartitions()
        tc.log(fmt.Sprintf("Level 1: Created %s%d%s coarse partitions",
                ColorBoldYellow, len(coarsePartitions), ColorReset))

        var allDifferences []Difference

        for _, coarsePartition := range coarsePartitions {
                if tc.partitionHashesMatch(ctx, coarsePartition) {
                        atomic.AddInt64(&tc.partitionsSkipped, 1)
                        continue
                }

                atomic.AddInt64(&tc.partitionsProcessed, 1)

                finePartitions := tc.createFinePartitions(coarsePartition)
                for _, finePartition := range finePartitions {
                        if tc.partitionHashesMatch(ctx, finePartition) {
                                atomic.AddInt64(&tc.partitionsSkipped, 1)
                                continue
                        }

                        atomic.AddInt64(&tc.partitionsProcessed, 1)

                        microPartitions := tc.createMicroPartitions(finePartition)
                        for _, microPartition := range microPartitions {
                                if tc.partitionHashesMatch(ctx, microPartition) {
                                        atomic.AddInt64(&tc.partitionsSkipped, 1)
                                        continue
                                }

                                atomic.AddInt64(&tc.partitionsProcessed, 1)

                                // Level 4: Nano partitions
                                nanoPartitions := tc.createNanoPartitions(microPartition)
                                for _, nanoPartition := range nanoPartitions {
                                        if tc.partitionHashesMatch(ctx, nanoPartition) {
                                                atomic.AddInt64(&tc.partitionsSkipped, 1)
                                                continue
                                        }

                                        atomic.AddInt64(&tc.partitionsProcessed, 1)

                                        // Row-level comparison
                                        partitionDiffs := tc.performRowLevelComparison(ctx, nanoPartition)
                                        allDifferences = append(allDifferences, partitionDiffs...)
                                }
                        }
                }
        }

        return allDifferences, nil
}

// Helper methods for hierarchical partitioning

func (tc *TableComparator) createCoarsePartitions() []Partition {
        totalRange := tc.sourceStats.MaxID - tc.sourceStats.MinID + 1
        partitionSize := totalRange / int64(tc.config.CoarsePartitions)

        if partitionSize < 1000 {
                partitionSize = 1000
        }

        var partitions []Partition
        currentID := tc.sourceStats.MinID

        for i := 0; i < tc.config.CoarsePartitions && currentID <= tc.sourceStats.MaxID; i++ {
                endID := currentID + partitionSize - 1
                if endID > tc.sourceStats.MaxID || i == tc.config.CoarsePartitions-1 {
                        endID = tc.sourceStats.MaxID
                }

                partitions = append(partitions, Partition{
                        StartID: currentID,
                        EndID:   endID,
                })

                currentID = endID + 1
        }

        return partitions
}

func (tc *TableComparator) createFinePartitions(coarsePartition Partition) []Partition {
        totalRange := coarsePartition.EndID - coarsePartition.StartID + 1
        partitionCount := max(1, int(totalRange/tc.config.FinePartitionSize))
        partitionSize := totalRange / int64(partitionCount)

        var partitions []Partition
        currentID := coarsePartition.StartID

        for currentID <= coarsePartition.EndID {
                endID := currentID + partitionSize - 1
                if endID > coarsePartition.EndID {
                        endID = coarsePartition.EndID
                }

                partitions = append(partitions, Partition{
                        StartID: currentID,
                        EndID:   endID,
                })

                currentID = endID + 1
        }

        return partitions
}

func (tc *TableComparator) createMicroPartitions(finePartition Partition) []Partition {
        totalRange := finePartition.EndID - finePartition.StartID + 1
        partitionCount := max(1, int(totalRange/tc.config.MicroPartitionSize))
        partitionSize := totalRange / int64(partitionCount)

        var partitions []Partition
        currentID := finePartition.StartID

        for currentID <= finePartition.EndID {
                endID := currentID + partitionSize - 1
                if endID > finePartition.EndID {
                        endID = finePartition.EndID
                }

                partitions = append(partitions, Partition{
                        StartID: currentID,
                        EndID:   endID,
                })

                currentID = endID + 1
        }

        return partitions
}

func (tc *TableComparator) createNanoPartitions(microPartition Partition) []Partition {
        totalRange := microPartition.EndID - microPartition.StartID + 1
        partitionSize := max(1000, int(totalRange/10)) // Split into ~10 nano partitions

        var partitions []Partition
        currentID := microPartition.StartID

        for currentID <= microPartition.EndID {
                endID := currentID + int64(partitionSize) - 1
                if endID > microPartition.EndID {
                        endID = microPartition.EndID
                }

                partitions = append(partitions, Partition{
                        StartID: currentID,
                        EndID:   endID,
                })

                currentID = endID + 1
        }

        return partitions
}

// Check if partition hashes match between source and target
func (tc *TableComparator) partitionHashesMatch(ctx context.Context, partition Partition) bool {
        sourceHash, err := tc.computePartitionHash(ctx, tc.sourcePool, partition)
        if err != nil {
                tc.log(fmt.Sprintf("Error computing source partition hash: %v", err))
                return false
        }

        targetHash, err := tc.computePartitionHash(ctx, tc.targetPool, partition)
        if err != nil {
                tc.log(fmt.Sprintf("Error computing target partition hash: %v", err))
                return false
        }

        return sourceHash == targetHash
}

// Compute aggregate hash for entire partition
func (tc *TableComparator) computePartitionHash(ctx context.Context, pool *pgxpool.Pool, partition Partition) (string, error) {
        // Build column list for hash (exclude primary key from hash)
        columns := []string{}
        for _, col := range tc.sourceStats.ColumnNames {
                if col != tc.sourceStats.PrimaryKey {
                        columns = append(columns, col)
                }
        }

        query := fmt.Sprintf(`
                SELECT MD5(string_agg(MD5(ROW(%s)::text), '' ORDER BY %s))
                FROM %s.%s
                WHERE %s >= $1 AND %s <= $2
        `,
                strings.Join(columns, ", "),
                tc.sourceStats.PrimaryKey,
                tc.config.SchemaName,
                tc.config.TableName,
                tc.sourceStats.PrimaryKey,
                tc.sourceStats.PrimaryKey,
        )

        var hash string
        err := pool.QueryRow(ctx, query, partition.StartID, partition.EndID).Scan(&hash)
        if err != nil {
                return "", fmt.Errorf("failed to compute partition hash: %w", err)
        }

        return hash, nil
}

// Perform row-level comparison for a small partition
func (tc *TableComparator) performRowLevelComparison(ctx context.Context, partition Partition) []Difference {
        // Use existing row-level comparison logic
        diffIDs := tc.comparePartition(ctx, partition)
        if len(diffIDs) == 0 {
                return []Difference{}
        }

        differences := tc.fetchDifferences(ctx, diffIDs)
        return differences
}

// Rest of the implementation remains the same as before...
// (gatherStatistics, createPartitions, comparePartition, fetchHashes, etc.)

func (tc *TableComparator) gatherStatistics(ctx context.Context) error {
        tc.log(tc.colorize("Gathering statistics (optimized - no COUNT queries)...", ColorPurple))

        var wg sync.WaitGroup
        var sourceErr, targetErr error

        statsCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
        defer cancel()

        wg.Add(2)

        go func() {
                defer wg.Done()
                tc.log(tc.colorize("Fetching source table statistics...", ColorBlue))
                sourceErr = tc.gatherTableStats(statsCtx, tc.sourcePool, &tc.sourceStats)
                if sourceErr == nil {
                        tc.log(fmt.Sprintf("%s stats complete: range %s%d-%d%s (~%s%d%s rows)",
                                tc.colorize("Source", ColorGreen),
                                ColorCyan, tc.sourceStats.MinID, tc.sourceStats.MaxID, ColorReset,
                                ColorBoldGreen, tc.sourceStats.RowCount, ColorReset))
                }
        }()

        go func() {
                defer wg.Done()
                tc.log(tc.colorize("Fetching target table statistics...", ColorBlue))
                targetErr = tc.gatherTableStats(statsCtx, tc.targetPool, &tc.targetStats)
                if targetErr == nil {
                        tc.log(fmt.Sprintf("%s stats complete: range %s%d-%d%s (~%s%d%s rows)",
                                tc.colorize("Target", ColorGreen),
                                ColorCyan, tc.targetStats.MinID, tc.targetStats.MaxID, ColorReset,
                                ColorBoldGreen, tc.targetStats.RowCount, ColorReset))
                }
        }()

        wg.Wait()

        if sourceErr != nil {
                return fmt.Errorf("source stats error: %w", sourceErr)
        }
        if targetErr != nil {
                return fmt.Errorf("target stats error: %w", targetErr)
        }

        return nil
}

func (tc *TableComparator) gatherTableStats(ctx context.Context, pool *pgxpool.Pool, stats **TableStats) error {
        *stats = &TableStats{}

        pkQuery := `
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                LIMIT 1
        `

        tableName := fmt.Sprintf("%s.%s", tc.config.SchemaName, tc.config.TableName)
        err := pool.QueryRow(ctx, pkQuery, tableName).Scan(&(*stats).PrimaryKey)
        if err != nil {
                return fmt.Errorf("failed to get primary key: %w", err)
        }

        rangeQuery := fmt.Sprintf(`
                SELECT
                        COALESCE(MIN(%s), 0) as min_id,
                        COALESCE(MAX(%s), 0) as max_id
                FROM %s
        `, (*stats).PrimaryKey, (*stats).PrimaryKey, tableName)

        err = pool.QueryRow(ctx, rangeQuery).Scan(
                &(*stats).MinID,
                &(*stats).MaxID,
        )
        if err != nil {
                return fmt.Errorf("failed to get ID range: %w", err)
        }

        if (*stats).MaxID >= (*stats).MinID {
                (*stats).RowCount = (*stats).MaxID - (*stats).MinID + 1
        } else {
                (*stats).RowCount = 0
        }

        colQuery := `
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
        `

        rows, err := pool.Query(ctx, colQuery, tc.config.SchemaName, tc.config.TableName)
        if err != nil {
                return fmt.Errorf("failed to get columns: %w", err)
        }
        defer rows.Close()

        for rows.Next() {
                var colName string
                if err := rows.Scan(&colName); err != nil {
                        return err
                }
                (*stats).ColumnNames = append((*stats).ColumnNames, colName)
        }

        return nil
}

func (tc *TableComparator) createPartitions() []Partition {
        if tc.sourceStats.MaxID == 0 || tc.sourceStats.MinID == tc.sourceStats.MaxID {
                return []Partition{{StartID: tc.sourceStats.MinID, EndID: tc.sourceStats.MaxID}}
        }

        totalRange := tc.sourceStats.MaxID - tc.sourceStats.MinID + 1
        partitionSize := totalRange / int64(tc.config.WorkerCount)

        if partitionSize < 1000 {
                partitionSize = 1000
        }

        var partitions []Partition
        currentID := tc.sourceStats.MinID

        for currentID <= tc.sourceStats.MaxID {
                endID := currentID + partitionSize - 1
                if endID > tc.sourceStats.MaxID {
                        endID = tc.sourceStats.MaxID
                }

                partitions = append(partitions, Partition{
                        StartID: currentID,
                        EndID:   endID,
                })

                currentID = endID + 1
        }

        return partitions
}

func (tc *TableComparator) performHashComparison(ctx context.Context, partitions []Partition) map[int64]DifferenceType {
        resultChan := make(chan map[int64]DifferenceType, len(partitions))
        semaphore := make(chan struct{}, tc.config.WorkerCount)

        var wg sync.WaitGroup

        for i, partition := range partitions {
                wg.Add(1)
                go func(workerID int, p Partition) {
                        defer wg.Done()

                        semaphore <- struct{}{}
                        defer func() { <-semaphore }()

                        tc.log(fmt.Sprintf("%sWorker %d%s: Processing range %s%d-%d%s",
                                ColorBoldBlue, workerID, ColorReset,
                                ColorCyan, p.StartID, p.EndID, ColorReset))

                        diffs := tc.comparePartition(ctx, p)
                        resultChan <- diffs

                        if len(diffs) > 0 {
                                tc.log(fmt.Sprintf("%sWorker %d%s: Found %s%d differences%s",
                                        ColorBoldBlue, workerID, ColorReset,
                                        ColorBoldYellow, len(diffs), ColorReset))
                        }
                }(i, partition)
        }

        go func() {
                wg.Wait()
                close(resultChan)
        }()

        allDiffs := make(map[int64]DifferenceType)
        for diffs := range resultChan {
                for id, diffType := range diffs {
                        allDiffs[id] = diffType
                }
        }

        return allDiffs
}

func (tc *TableComparator) comparePartition(ctx context.Context, partition Partition) map[int64]DifferenceType {
        differences := make(map[int64]DifferenceType)

        var sourceHashes, targetHashes map[int64]string
        var sourceErr, targetErr error
        var wg sync.WaitGroup

        wg.Add(2)

        go func() {
                defer wg.Done()
                sourceHashes, sourceErr = tc.fetchHashes(ctx, tc.sourcePool, partition)
        }()

        go func() {
                defer wg.Done()
                targetHashes, targetErr = tc.fetchHashes(ctx, tc.targetPool, partition)
        }()

        wg.Wait()

        if sourceErr != nil || targetErr != nil {
                tc.log(fmt.Sprintf("%sError fetching hashes%s: source=%v, target=%v",
                        ColorBoldRed, ColorReset, sourceErr, targetErr))
                return differences
        }

        for id, sourceHash := range sourceHashes {
                atomic.AddInt64(&tc.rowsChecked, 1)

                targetHash, exists := targetHashes[id]
                if !exists {
                        differences[id] = Missing
                } else if sourceHash != targetHash {
                        differences[id] = Modified
                }
        }

        for id := range targetHashes {
                if _, exists := sourceHashes[id]; !exists {
                        differences[id] = Extra
                }
        }

        return differences
}

func (tc *TableComparator) fetchHashes(ctx context.Context, pool *pgxpool.Pool, partition Partition) (map[int64]string, error) {
        hashes := make(map[int64]string)

        columns := []string{}
        for _, col := range tc.sourceStats.ColumnNames {
                if col != tc.sourceStats.PrimaryKey {
                        columns = append(columns, col)
                }
        }

        query := fmt.Sprintf(`
                SELECT
                        %s as id,
                        MD5(ROW(%s)::text) as hash
                FROM %s.%s
                WHERE %s >= $1 AND %s <= $2
                ORDER BY %s
        `,
                tc.sourceStats.PrimaryKey,
                strings.Join(columns, ", "),
                tc.config.SchemaName,
                tc.config.TableName,
                tc.sourceStats.PrimaryKey,
                tc.sourceStats.PrimaryKey,
                tc.sourceStats.PrimaryKey,
        )

        rows, err := pool.Query(ctx, query, partition.StartID, partition.EndID)
        if err != nil {
                return nil, fmt.Errorf("failed to fetch hashes: %w", err)
        }
        defer rows.Close()

        for rows.Next() {
                var id int64
                var hash string
                if err := rows.Scan(&id, &hash); err != nil {
                        return nil, err
                }
                hashes[id] = hash
        }

        return hashes, nil
}

func (tc *TableComparator) fetchDifferences(ctx context.Context, diffIDs map[int64]DifferenceType) []Difference {
        var differences []Difference
        var mu sync.Mutex

        var missingIDs, extraIDs, modifiedIDs []int64

        for id, diffType := range diffIDs {
                switch diffType {
                case Missing:
                        missingIDs = append(missingIDs, id)
                case Extra:
                        extraIDs = append(extraIDs, id)
                case Modified:
                        modifiedIDs = append(modifiedIDs, id)
                }
        }

        var wg sync.WaitGroup

        if len(missingIDs) > 0 {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        rows := tc.fetchRowsByIDs(ctx, tc.sourcePool, missingIDs)
                        mu.Lock()
                        for _, row := range rows {
                                differences = append(differences, Difference{
                                        Type:       Missing,
                                        ID:         row["id"].(int64),
                                        SourceData: row,
                                })
                        }
                        mu.Unlock()
                }()
        }

        if len(extraIDs) > 0 {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        rows := tc.fetchRowsByIDs(ctx, tc.targetPool, extraIDs)
                        mu.Lock()
                        for _, row := range rows {
                                differences = append(differences, Difference{
                                        Type:       Extra,
                                        ID:         row["id"].(int64),
                                        TargetData: row,
                                })
                        }
                        mu.Unlock()
                }()
        }

        if len(modifiedIDs) > 0 {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        tc.fetchModifiedRows(ctx, modifiedIDs, &differences, &mu)
                }()
        }

        wg.Wait()

        return differences
}

func (tc *TableComparator) fetchRowsByIDs(ctx context.Context, pool *pgxpool.Pool, ids []int64) []map[string]interface{} {
        var allRows []map[string]interface{}

        batchSize := 1000
        for i := 0; i < len(ids); i += batchSize {
                end := i + batchSize
                if end > len(ids) {
                        end = len(ids)
                }

                batch := ids[i:end]
                rows := tc.fetchBatch(ctx, pool, batch)
                allRows = append(allRows, rows...)
        }

        return allRows
}

func (tc *TableComparator) fetchBatch(ctx context.Context, pool *pgxpool.Pool, ids []int64) []map[string]interface{} {
        idStrs := make([]string, len(ids))
        for i, id := range ids {
                idStrs[i] = fmt.Sprintf("%d", id)
        }

        query := fmt.Sprintf(`
                SELECT * FROM %s.%s
                WHERE %s IN (%s)
        `,
                tc.config.SchemaName,
                tc.config.TableName,
                tc.sourceStats.PrimaryKey,
                strings.Join(idStrs, ","),
        )

        rows, err := pool.Query(ctx, query)
        if err != nil {
                tc.log(fmt.Sprintf("%sError fetching batch%s: %v", ColorBoldRed, ColorReset, err))
                return nil
        }
        defer rows.Close()

        var results []map[string]interface{}

        for rows.Next() {
                values, err := rows.Values()
                if err != nil {
                        continue
                }

                rowMap := make(map[string]interface{})
                fieldDescs := rows.FieldDescriptions()
                for i := 0; i < len(fieldDescs); i++ {
                        colName := string(fieldDescs[i].Name)
                        rowMap[colName] = values[i]
                        if colName == tc.sourceStats.PrimaryKey {
                                rowMap["id"] = values[i]
                        }
                }

                results = append(results, rowMap)
        }

        return results
}

func (tc *TableComparator) fetchModifiedRows(ctx context.Context, ids []int64, differences *[]Difference, mu *sync.Mutex) {
        sourceRows := tc.fetchRowsByIDs(ctx, tc.sourcePool, ids)
        targetRows := tc.fetchRowsByIDs(ctx, tc.targetPool, ids)

        sourceMap := make(map[int64]map[string]interface{})
        for _, row := range sourceRows {
                if id, ok := row["id"].(int64); ok {
                        sourceMap[id] = row
                }
        }

        targetMap := make(map[int64]map[string]interface{})
        for _, row := range targetRows {
                if id, ok := row["id"].(int64); ok {
                        targetMap[id] = row
                }
        }

        for _, id := range ids {
                sourceRow := sourceMap[id]
                targetRow := targetMap[id]

                if sourceRow != nil && targetRow != nil {
                        changes := tc.compareRows(sourceRow, targetRow)
                        if len(changes) > 0 {
                                mu.Lock()
                                *differences = append(*differences, Difference{
                                        Type:       Modified,
                                        ID:         id,
                                        SourceData: sourceRow,
                                        TargetData: targetRow,
                                        Changes:    changes,
                                })
                                mu.Unlock()
                        }
                }
        }
}

func (tc *TableComparator) compareRows(source, target map[string]interface{}) map[string]ChangeDetail {
        changes := make(map[string]ChangeDetail)

        for key, sourceVal := range source {
                if key == "id" || key == tc.sourceStats.PrimaryKey {
                        continue
                }

                targetVal := target[key]
                if !tc.valuesEqual(sourceVal, targetVal) {
                        changes[key] = ChangeDetail{
                                OldValue: sourceVal,
                                NewValue: targetVal,
                        }
                }
        }

        return changes
}

func (tc *TableComparator) valuesEqual(a, b interface{}) bool {
        if a == nil && b == nil {
                return true
        }
        if a == nil || b == nil {
                return false
        }

        return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func (tc *TableComparator) buildResult(differences []Difference) *ComparisonResult {
        result := &ComparisonResult{}

        result.MissingRows = []Difference{}
        result.ExtraRows = []Difference{}
        result.ModifiedRows = []Difference{}

        for _, diff := range differences {
                switch diff.Type {
                case Missing:
                        result.MissingRows = append(result.MissingRows, diff)
                case Extra:
                        result.ExtraRows = append(result.ExtraRows, diff)
                case Modified:
                        result.ModifiedRows = append(result.ModifiedRows, diff)
                }
        }

        result.Summary.SourceDB = tc.config.SourceURL
        result.Summary.TargetDB = tc.config.TargetURL
        result.Summary.Table = fmt.Sprintf("%s.%s", tc.config.SchemaName, tc.config.TableName)
        result.Summary.TotalSourceRows = tc.sourceStats.RowCount
        result.Summary.TotalTargetRows = tc.targetStats.RowCount
        result.Summary.RowsChecked = atomic.LoadInt64(&tc.rowsChecked)
        result.Summary.MissingCount = len(result.MissingRows)
        result.Summary.ExtraCount = len(result.ExtraRows)
        result.Summary.ModifiedCount = len(result.ModifiedRows)

        totalDiffs := float64(result.Summary.MissingCount + result.Summary.ExtraCount + result.Summary.ModifiedCount)
        checkedRows := float64(atomic.LoadInt64(&tc.rowsChecked))
        if checkedRows > 0 {
                result.Summary.MatchPercentage = ((checkedRows - totalDiffs) / checkedRows) * 100
        }

        return result
}

// SQL Fix Generation Methods (same as before)
func (tc *TableComparator) generateSQLFixes(result *ComparisonResult) string {
        var sqlStatements []string
        tableName := fmt.Sprintf("%s.%s", tc.config.SchemaName, tc.config.TableName)

        sqlStatements = append(sqlStatements, "-- Database Table Comparison Fix Script")
        sqlStatements = append(sqlStatements, fmt.Sprintf("-- Generated: %s", time.Now().Format("2006-01-02 15:04:05")))
        sqlStatements = append(sqlStatements, fmt.Sprintf("-- Table: %s", tableName))
        sqlStatements = append(sqlStatements, fmt.Sprintf("-- Strategy: %s", result.Summary.Strategy))
        sqlStatements = append(sqlStatements, fmt.Sprintf("-- Total Fixes: %d",
                len(result.MissingRows)+len(result.ExtraRows)+len(result.ModifiedRows)))
        sqlStatements = append(sqlStatements, "")
        sqlStatements = append(sqlStatements, "BEGIN;")
        sqlStatements = append(sqlStatements, "")

        if len(result.MissingRows) > 0 {
                sqlStatements = append(sqlStatements, fmt.Sprintf("-- Missing Rows: Insert %d rows into target", len(result.MissingRows)))
                for _, diff := range result.MissingRows {
                        insertSQL := tc.generateInsertSQL(tableName, diff.SourceData)
                        sqlStatements = append(sqlStatements, insertSQL)
                }
                sqlStatements = append(sqlStatements, "")
        }

        if len(result.ExtraRows) > 0 {
                sqlStatements = append(sqlStatements, fmt.Sprintf("-- Extra Rows: Delete %d rows from target", len(result.ExtraRows)))
                for _, diff := range result.ExtraRows {
                        deleteSQL := tc.generateDeleteSQL(tableName, diff.ID)
                        sqlStatements = append(sqlStatements, deleteSQL)
                }
                sqlStatements = append(sqlStatements, "")
        }

        if len(result.ModifiedRows) > 0 {
                sqlStatements = append(sqlStatements, fmt.Sprintf("-- Modified Rows: Update %d rows in target", len(result.ModifiedRows)))
                for _, diff := range result.ModifiedRows {
                        updateSQL := tc.generateUpdateSQL(tableName, diff.ID, diff.SourceData, diff.Changes)
                        if updateSQL != "" {
                                sqlStatements = append(sqlStatements, updateSQL)
                        }
                }
                sqlStatements = append(sqlStatements, "")
        }

        sqlStatements = append(sqlStatements, "COMMIT;")
        sqlStatements = append(sqlStatements, "")
        sqlStatements = append(sqlStatements, fmt.Sprintf("-- Fix script completed. Modified %d rows total.",
                len(result.MissingRows)+len(result.ExtraRows)+len(result.ModifiedRows)))

        return strings.Join(sqlStatements, "\n")
}

func (tc *TableComparator) generateInsertSQL(tableName string, rowData map[string]interface{}) string {
        var columns []string
        var values []string

        for _, colName := range tc.sourceStats.ColumnNames {
                if value, exists := rowData[colName]; exists {
                        columns = append(columns, colName)
                        values = append(values, tc.formatSQLValue(value))
                }
        }

        return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
                tableName,
                strings.Join(columns, ", "),
                strings.Join(values, ", "))
}

func (tc *TableComparator) generateDeleteSQL(tableName string, id int64) string {
        return fmt.Sprintf("DELETE FROM %s WHERE %s = %d;",
                tableName, tc.sourceStats.PrimaryKey, id)
}

func (tc *TableComparator) generateUpdateSQL(tableName string, id int64, sourceData map[string]interface{}, changes map[string]ChangeDetail) string {
        if len(changes) == 0 {
                return ""
        }

        var setParts []string
        for fieldName := range changes {
                if fieldName == "id" {
                        continue
                }
                if sourceValue, exists := sourceData[fieldName]; exists {
                        setParts = append(setParts, fmt.Sprintf("%s = %s",
                                fieldName, tc.formatSQLValue(sourceValue)))
                }
        }

        if len(setParts) == 0 {
                return ""
        }

        return fmt.Sprintf("UPDATE %s SET %s WHERE %s = %d;",
                tableName,
                strings.Join(setParts, ", "),
                tc.sourceStats.PrimaryKey,
                id)
}

func (tc *TableComparator) formatSQLValue(value interface{}) string {
        if value == nil {
                return "NULL"
        }

        switch v := value.(type) {
        case string:
                escaped := strings.ReplaceAll(v, "'", "''")
                return fmt.Sprintf("'%s'", escaped)
        case int, int8, int16, int32, int64:
                return fmt.Sprintf("%d", v)
        case uint, uint8, uint16, uint32, uint64:
                return fmt.Sprintf("%d", v)
        case float32, float64:
                return fmt.Sprintf("%f", v)
        case bool:
                if v {
                        return "TRUE"
                }
                return "FALSE"
        case time.Time:
                return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.000000"))
        case []byte:
                return fmt.Sprintf("'\\x%x'", v)
        case map[string]interface{}, []interface{}, map[string]string:
                jsonBytes, err := json.Marshal(v)
                if err != nil {
                        return "NULL"
                }
                escaped := strings.ReplaceAll(string(jsonBytes), "'", "''")
                return fmt.Sprintf("'%s'", escaped)
        default:
                str := fmt.Sprintf("%v", v)
                if strings.Contains(str, "map[") || strings.Contains(str, "[]") {
                        if jsonBytes, err := json.Marshal(v); err == nil {
                                escaped := strings.ReplaceAll(string(jsonBytes), "'", "''")
                                return fmt.Sprintf("'%s'", escaped)
                        }
                        return "NULL"
                }
                escaped := strings.ReplaceAll(str, "'", "''")
                return fmt.Sprintf("'%s'", escaped)
        }
}


func (tc *TableComparator) outputResults(result *ComparisonResult) error {
        tc.printSummary(result)

        if tc.config.OutputFile != "" {
                data, err := json.MarshalIndent(result, "", "  ")
                if err != nil {
                        return fmt.Errorf("failed to marshal results: %w", err)
                }

                if err := os.WriteFile(tc.config.OutputFile, data, 0644); err != nil {
                        return fmt.Errorf("failed to write output file: %w", err)
                }

                fmt.Printf("\n%s %s%s%s\n",
                        tc.colorize("✓ Detailed results saved to:", ColorBoldGreen),
                        ColorBoldCyan, tc.config.OutputFile, ColorReset)
        }

        if tc.config.SQLFixFile != "" {
                totalFixes := len(result.MissingRows) + len(result.ExtraRows) + len(result.ModifiedRows)
                if totalFixes > 0 {
                        sqlScript := tc.generateSQLFixes(result)
                        if err := os.WriteFile(tc.config.SQLFixFile, []byte(sqlScript), 0644); err != nil {
                                return fmt.Errorf("failed to write SQL fix file: %w", err)
                        }

                        fmt.Printf("%s %s%s%s (%s%d fixes%s)\n",
                                tc.colorize("🔧 SQL fix script generated:", ColorBoldYellow),
                                ColorBoldCyan, tc.config.SQLFixFile, ColorReset,
                                ColorBoldYellow, totalFixes, ColorReset)
                } else {
                        fmt.Printf("%s\n",
                                tc.colorize("✓ No differences found - no SQL fix script needed", ColorBoldGreen))
                }
        }

        return nil
}

func (tc *TableComparator) printSummary(result *ComparisonResult) {
        separator := strings.Repeat("=", 60)
        fmt.Printf("\n%s%s%s\n", ColorBoldCyan, separator, ColorReset)
        fmt.Printf("%s%s%s\n", ColorBoldCyan, "DATABASE COMPARISON REPORT", ColorReset)
        fmt.Printf("%s%s%s\n", ColorBoldCyan, separator, ColorReset)

        fmt.Printf("Table: %s%s%s\n", ColorBoldPurple, result.Summary.Table, ColorReset)
        fmt.Printf("Strategy: %s%s%s\n", ColorBoldCyan, result.Summary.Strategy, ColorReset)
        fmt.Printf("Source Estimated Rows: %s%d%s\n", ColorBoldGreen, result.Summary.TotalSourceRows, ColorReset)
        fmt.Printf("Target Estimated Rows: %s%d%s\n", ColorBoldGreen, result.Summary.TotalTargetRows, ColorReset)
        fmt.Printf("Actual Rows Checked: %s%d%s\n", ColorBoldBlue, result.Summary.RowsChecked, ColorReset)

        if result.Summary.PartitionsSkipped > 0 || result.Summary.PartitionsProcessed > 0 {
                fmt.Printf("Partitions Skipped/Processed: %s%d%s / %s%d%s\n",
                        ColorBoldGreen, result.Summary.PartitionsSkipped, ColorReset,
                        ColorBoldYellow, result.Summary.PartitionsProcessed, ColorReset)
        }

        fmt.Printf("%s%s%s\n", ColorCyan, strings.Repeat("-", 60), ColorReset)

        if result.Summary.MissingCount > 0 {
                fmt.Printf("%s❌ MISSING IN TARGET: %d rows%s\n",
                        ColorBoldRed, result.Summary.MissingCount, ColorReset)
                if tc.config.Verbose && len(result.MissingRows) <= 10 {
                        for _, row := range result.MissingRows {
                                fmt.Printf("   %s- ID: %d%s\n", ColorRed, row.ID, ColorReset)
                        }
                }
        }

        if result.Summary.ExtraCount > 0 {
                fmt.Printf("%s⚠️  EXTRA IN TARGET: %d rows%s\n",
                        ColorBoldYellow, result.Summary.ExtraCount, ColorReset)
                if tc.config.Verbose && len(result.ExtraRows) <= 10 {
                        for _, row := range result.ExtraRows {
                                fmt.Printf("   %s- ID: %d%s\n", ColorYellow, row.ID, ColorReset)
                        }
                }
        }

        if result.Summary.ModifiedCount > 0 {
                fmt.Printf("%s🔄 MODIFIED ROWS: %d rows%s\n",
                        ColorBoldBlue, result.Summary.ModifiedCount, ColorReset)
                if tc.config.Verbose && len(result.ModifiedRows) <= 10 {
                        for _, row := range result.ModifiedRows {
                                fmt.Printf("   %s- ID: %d%s\n", ColorBlue, row.ID, ColorReset)
                                for field, change := range row.Changes {
                                        fmt.Printf("     %s%s%s: %s%v%s → %s%v%s\n",
                                                ColorPurple, field, ColorReset,
                                                ColorRed, change.OldValue, ColorReset,
                                                ColorGreen, change.NewValue, ColorReset)
                                }
                        }
                }
        }

        fmt.Printf("%s%s%s\n", ColorCyan, strings.Repeat("-", 60), ColorReset)

        if result.Summary.MatchPercentage >= 95.0 {
                fmt.Printf("%s✓ Match Percentage: %.2f%%%s\n",
                        ColorBoldGreen, result.Summary.MatchPercentage, ColorReset)
        } else if result.Summary.MatchPercentage >= 80.0 {
                fmt.Printf("%s⚠️  Match Percentage: %.2f%%%s\n",
                        ColorBoldYellow, result.Summary.MatchPercentage, ColorReset)
        } else {
                fmt.Printf("%s❌ Match Percentage: %.2f%%%s\n",
                        ColorBoldRed, result.Summary.MatchPercentage, ColorReset)
        }

        fmt.Printf("%s%s%s\n", ColorBoldCyan, separator, ColorReset)
}

func (tc *TableComparator) progressReporter() {
        ticker := time.NewTicker(10 * time.Second)
        defer ticker.Stop()

        for {
                select {
                case msg := <-tc.progressChan:
                        if tc.config.Verbose {
                                fmt.Printf("[%s%s%s] %s\n",
                                        ColorBoldGreen, time.Now().Format("15:04:05"), ColorReset, msg)
                        }
                case <-ticker.C:
                        checked := atomic.LoadInt64(&tc.rowsChecked)
                        if checked > 0 {
                                fmt.Printf("%sProgress%s: %s%d%s rows checked\n",
                                        ColorBoldCyan, ColorReset,
                                        ColorBoldGreen, checked, ColorReset)
                        }
                }
        }
}

func (tc *TableComparator) log(message string) {
        select {
        case tc.progressChan <- message:
        default:
        }
}