
import aiohttp
import asyncio
import time
import random
import argparse
import shutil
import os
from pathlib import Path
import statistics
from typing import AsyncGenerator, AsyncIterator, Any, NamedTuple
from dataclasses import dataclass


@dataclass
class BenchmarkConfig:
    num_docs: int = 10_000
    num_searches: int = 1_000
    num_runs: int = 3
    server_exe: str = 'zig-out/bin/fpindex'
    server_port: int = 8999
    index_name: str = 'benchmark_index'
    data_dir_base: str = '/tmp/fpindex_benchmark'
    max_concurrent_searches: int = 16
    server_ready_timeout: float = 15.0
    min_hashes_per_fingerprint: int = 100
    max_hashes_per_fingerprint: int = 120
    max_fp_id: int = (1 << 32) - 1
    merge_wait_time: float = 5.0
    random_seed: int = 42
    
    # Hash space configuration (24-bit hashes by default for more collisions)
    hash_bits: int = 24
    
    # Insert mode configurations
    bulk_batch_size: int = 1000
    medium_batch_min: int = 10
    medium_batch_max: int = 100
    small_batch_min: int = 1
    small_batch_max: int = 10
    
    # Search mode configurations
    searches_per_mode: int = 333  # Will do 3 modes, so total ~num_searches
    match_probability_high: float = 0.9  # For "mostly matches" mode
    match_probability_low: float = 0.1   # For "mostly misses" mode
    match_probability_mixed: float = 0.5 # For "mixed" mode
    
    @property
    def max_hash_value(self) -> int:
        """Calculate maximum hash value based on hash_bits."""
        return (1 << self.hash_bits) - 1
    
    def get_hash_space_info(self) -> str:
        """Get human-readable hash space information."""
        return f"{self.hash_bits}-bit hashes (0 to {self.max_hash_value:,}, ~{self.max_hash_value/1_000_000:.1f}M values)"


def mean(data: list[float]) -> float:
    if not data:
        return 0.0
    return statistics.mean(data)


def std(data: list[float]) -> float:
    if len(data) < 2:
        return 0.0
    return statistics.stdev(data)


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    # Use statistics.quantiles with method='inclusive' for better compatibility
    quantiles = statistics.quantiles(data, n=100, method='inclusive')
    # quantiles[0] is 1st percentile, quantiles[94] is 95th percentile  
    index = int(p) - 1
    if index < 0:
        return min(data)
    elif index >= len(quantiles):
        return max(data)
    else:
        return quantiles[index]


class ServerManager:
    def __init__(self, base_dir: str, port: int, config: BenchmarkConfig):
        self.data_dir = Path(base_dir)
        self.log_file = self.data_dir / 'server.log'
        self.port = port
        self.config = config
        self.process: asyncio.subprocess.Process | None = None

    async def __aenter__(self):
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True)
        
        command = [
            self.config.server_exe,
            '--dir', str(self.data_dir),
            '--port', str(self.port),
            '--log-level', 'debug',
        ]
        print(' '.join(command))
        
        # Open log file for stderr redirection
        stderr_file = self.log_file.open('a')
        self.process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=stderr_file,
        )
        await self.wait_for_ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        if exc_type is not None:
            self.print_error_log()

    async def wait_for_ready(self, timeout: float | None = None) -> None:
        if timeout is None:
            timeout = self.config.server_ready_timeout
        deadline = time.time() + timeout
        while True:
            url = f'http://localhost:{self.port}/_health'
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as res:
                        if res.status == 200:
                            return
            except aiohttp.ClientConnectorError:
                pass
            
            if time.time() > deadline:
                raise TimeoutError("Server did not become ready in time.")

            if self.process is not None and self.process.returncode is not None:
                raise ConnectionError("Server process died.")

            await asyncio.sleep(0.1)

    def get_data_size(self) -> int:
        return sum(f.stat().st_size for f in self.data_dir.glob('**/*') if f.is_file())

    def print_error_log(self) -> None:
        if self.log_file.exists():
            print("--- Server Log ---")
            with self.log_file.open('r') as f:
                print(f.read())
            print("--------------------")


async def create_index(session: aiohttp.ClientSession, index_name: str, port: int) -> None:
    url = f'http://localhost:{port}/{index_name}'
    async with session.put(url) as response:
        print(await response.text())
        response.raise_for_status()


class FingerprintGenerator:
    """Manages fingerprint generation with consistent seeding for reproducible results."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.rng = random.Random(config.random_seed)
        self.inserted_ids: list[int] = []
        self.inserted_hashes: dict[int, list[int]] = {}
    
    def generate_fingerprint(self) -> dict[str, Any]:
        """Generate a single fingerprint with consistent randomization."""
        hashes = []
        for _ in range(self.rng.randint(self.config.min_hashes_per_fingerprint, self.config.max_hashes_per_fingerprint)):
            hashes.append(self.rng.randint(0, self.config.max_hash_value))
        fp_id = self.rng.randint(0, self.config.max_fp_id)
        
        # Track inserted data
        self.inserted_ids.append(fp_id)
        self.inserted_hashes[fp_id] = hashes
        
        return {'insert': {'id': fp_id, 'hashes': hashes}}
    
    def get_search_hashes(self, should_match: bool) -> list[int]:
        """Generate search hashes that either match or don't match inserted data.
        
        For matches: Uses exact subsets of inserted fingerprint hashes.
        For misses: Uses completely random hashes that don't exist in any fingerprint.
        """
        if should_match and self.inserted_ids:
            # Use exact subset of hashes from an existing fingerprint
            fp_id = self.rng.choice(self.inserted_ids)
            base_hashes = self.inserted_hashes[fp_id]
            
            # Create search query as exact subset (no noise/modification)
            # Use 50-90% of the original hashes for subset matching
            num_hashes = len(base_hashes)
            subset_size = self.rng.randint(
                max(1, int(num_hashes * 0.5)),  # At least 50% of hashes
                max(1, int(num_hashes * 0.9))   # Up to 90% of hashes
            )
            
            # Randomly sample exact hashes (no modification)
            exact_subset = self.rng.sample(base_hashes, subset_size)
            
            # Optionally add some additional random hashes to make query larger
            # This simulates a query that contains the fingerprint as a subset
            additional_count = self.rng.randint(0, 20)  # 0-20 additional random hashes
            for _ in range(additional_count):
                # Add random hash that might not exist in the fingerprint
                exact_subset.append(self.rng.randint(0, self.config.max_hash_value))
            
            # Debug: Print info about the search being generated
            if len(self.inserted_ids) <= 10:  # Only print for small datasets
                print(f"    DEBUG: Generating match search from FP {fp_id}, using {subset_size}/{len(base_hashes)} hashes + {additional_count} random")
            
            return exact_subset
        else:
            # Generate completely random hashes (should not match)
            # These should not have significant overlap with any inserted fingerprint
            hashes = []
            for _ in range(self.rng.randint(self.config.min_hashes_per_fingerprint, self.config.max_hashes_per_fingerprint)):
                hashes.append(self.rng.randint(0, self.config.max_hash_value))
            return hashes


async def generate_fingerprints(num_docs: int, generator: FingerprintGenerator) -> AsyncGenerator[dict[str, Any], None]:
    for i in range(1, num_docs + 1):
        yield generator.generate_fingerprint()


async def insert_batch(session: aiohttp.ClientSession, url: str, batch: list[dict[str, Any]]) -> None:
    """Helper function to insert a batch of fingerprints."""
    if not batch:
        return
    content = {'changes': batch}
    async with session.post(url, json=content) as response:
        response.raise_for_status()


async def run_insertion_mode(session: aiohttp.ClientSession, url: str, 
                            fingerprint_generator: AsyncIterator[dict[str, Any]], num_docs: int, 
                            mode_name: str, batch_size_range: tuple[int, int], rng: random.Random) -> tuple[float, float]:
    """Run insertion for a specific mode with given batch size range."""
    print(f"Inserting {num_docs} documents in {mode_name} mode...")
    start_time = time.time()
    batch = []
    
    for _ in range(num_docs):
        try:
            fingerprint = await fingerprint_generator.__anext__()
        except StopAsyncIteration:
            break
        batch.append(fingerprint)
        
        # Determine batch size based on mode
        if batch_size_range[0] == batch_size_range[1]:
            target_batch_size = batch_size_range[0]
        else:
            # Use the provided RNG for reproducibility
            target_batch_size = rng.randint(batch_size_range[0], batch_size_range[1])
        if len(batch) >= target_batch_size:
            await insert_batch(session, url, batch)
            batch = []
    
    # Insert remaining batch
    await insert_batch(session, url, batch)
    
    duration = time.time() - start_time
    rate = num_docs / duration if duration > 0 else 0
    print(f"{mode_name} insertion finished in {duration:.2f}s ({rate:.2f} docs/s)")
    
    return duration, rate


async def run_insertion(session: aiohttp.ClientSession, index_name: str, num_docs: int, config: BenchmarkConfig, fp_generator: FingerprintGenerator) -> dict[str, float]:
    print(f"Inserting {num_docs} documents across three modes...")
    print(f"DEBUG: Starting insertion, generator currently has {len(fp_generator.inserted_ids)} fingerprints tracked")
    url = f'http://localhost:{config.server_port}/{index_name}/_update'
    
    # Split documents across three modes
    docs_per_mode = num_docs // 3
    remaining_docs = num_docs % 3
    
    bulk_docs = docs_per_mode + (1 if remaining_docs > 0 else 0)
    medium_docs = docs_per_mode + (1 if remaining_docs > 1 else 0)
    small_docs = docs_per_mode
    
    fingerprint_generator = generate_fingerprints(num_docs, fp_generator).__aiter__()
    
    # Mode 1: Bulk load (large batches)
    bulk_duration, bulk_rate = await run_insertion_mode(
        session, url, fingerprint_generator, bulk_docs,
        "bulk load", (config.bulk_batch_size, config.bulk_batch_size), fp_generator.rng
    )
    
    # Mode 2: Medium batches
    medium_duration, medium_rate = await run_insertion_mode(
        session, url, fingerprint_generator, medium_docs,
        "medium batch", (config.medium_batch_min, config.medium_batch_max), fp_generator.rng
    )
    
    # Mode 3: Small batches
    small_duration, small_rate = await run_insertion_mode(
        session, url, fingerprint_generator, small_docs,
        "small batch", (config.small_batch_min, config.small_batch_max), fp_generator.rng
    )
    
    total_duration = bulk_duration + medium_duration + small_duration
    overall_rate = num_docs / total_duration if total_duration > 0 else 0
    
    print(f"Overall insertion finished in {total_duration:.2f}s ({overall_rate:.2f} docs/s)")
    print(f"DEBUG: After insertion, generator now has {len(fp_generator.inserted_ids)} fingerprints tracked")
    
    return {
        'bulk_insertion_time': bulk_duration,
        'bulk_insertion_rate': bulk_rate,
        'bulk_docs_count': bulk_docs,
        'medium_insertion_time': medium_duration,
        'medium_insertion_rate': medium_rate,
        'medium_docs_count': medium_docs,
        'small_insertion_time': small_duration,
        'small_insertion_rate': small_rate,
        'small_docs_count': small_docs,
        'total_insertion_time': total_duration,
        'overall_insertion_rate': overall_rate,
    }


class SearchResult(NamedTuple):
    """Result of a single search operation."""
    latency: float
    has_matches: bool
    expected_match: bool
    
    @property
    def is_true_positive(self) -> bool:
        """Expected match and got matches."""
        return self.expected_match and self.has_matches
    
    @property
    def is_true_negative(self) -> bool:
        """Expected no match and got no matches."""
        return not self.expected_match and not self.has_matches
    
    @property
    def is_false_positive(self) -> bool:
        """Expected no match but got matches."""
        return not self.expected_match and self.has_matches
    
    @property
    def is_false_negative(self) -> bool:
        """Expected match but got no matches."""
        return self.expected_match and not self.has_matches


class SearchModeResults(NamedTuple):
    """Results for a complete search mode."""
    avg_latency: float
    p95_latency: float
    total_searches: int
    matches_found: int
    true_positives: int
    true_negatives: int
    false_positives: int
    false_negatives: int
    expected_matches: int
    
    @property
    def accuracy(self) -> float:
        """(TP + TN) / Total"""
        return (self.true_positives + self.true_negatives) / self.total_searches
    
    @property
    def precision(self) -> float:
        """TP / (TP + FP), avoid division by zero"""
        denominator = self.true_positives + self.false_positives
        return self.true_positives / denominator if denominator > 0 else 0.0
    
    @property
    def recall(self) -> float:
        """TP / (TP + FN), avoid division by zero"""
        denominator = self.true_positives + self.false_negatives
        return self.true_positives / denominator if denominator > 0 else 0.0
    
    @property
    def f1_score(self) -> float:
        """2 * (precision * recall) / (precision + recall)"""
        p, r = self.precision, self.recall
        return 2 * (p * r) / (p + r) if (p + r) > 0 else 0.0


async def run_search_mode(session: aiohttp.ClientSession, url: str, mode_name: str, 
                         num_searches: int, match_probability: float, 
                         fp_generator: FingerprintGenerator, config: BenchmarkConfig) -> SearchModeResults:
    """Run searches for a specific mode (mostly matches, mostly misses, or mixed)."""
    print(f"Performing {num_searches} searches in {mode_name} mode (match probability: {match_probability:.1%})...")
    
    sem = asyncio.Semaphore(config.max_concurrent_searches)

    async def do_search(h: list[int], expected_match: bool) -> SearchResult:
        async with sem:
            start_time = time.time()
            async with session.post(url, json={'query': h}) as response:
                response.raise_for_status()
                result = await response.json()
            duration = time.time() - start_time
            
            # Check if we got any matches
            has_matches = len(result.get('results', [])) > 0
            
            return SearchResult(
                latency=duration,
                has_matches=has_matches,
                expected_match=expected_match
            )

    search_tasks = []
    expected_matches = 0
    
    for _ in range(num_searches):
        # Decide if this search should find a match
        should_match = fp_generator.rng.random() < match_probability
        if should_match:
            expected_matches += 1
        hashes = fp_generator.get_search_hashes(should_match)
        search_tasks.append(do_search(hashes, should_match))

    results = await asyncio.gather(*search_tasks)
    
    # Calculate metrics
    latencies = [r.latency for r in results]
    avg_latency = mean(latencies)
    p95_latency = percentile(latencies, 95)
    
    matches_found = sum(1 for r in results if r.has_matches)
    true_positives = sum(1 for r in results if r.is_true_positive)
    true_negatives = sum(1 for r in results if r.is_true_negative)
    false_positives = sum(1 for r in results if r.is_false_positive)
    false_negatives = sum(1 for r in results if r.is_false_negative)
    
    search_mode_results = SearchModeResults(
        avg_latency=avg_latency,
        p95_latency=p95_latency,
        total_searches=num_searches,
        matches_found=matches_found,
        true_positives=true_positives,
        true_negatives=true_negatives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        expected_matches=expected_matches
    )
    
    print(f"{mode_name} searches finished. Avg latency: {avg_latency*1000:.2f}ms, p95 latency: {p95_latency*1000:.2f}ms")
    print(f"  Expected matches: {expected_matches}/{num_searches} ({expected_matches/num_searches:.1%})")
    print(f"  Actual matches: {matches_found}/{num_searches} ({matches_found/num_searches:.1%})")
    print(f"  TP: {true_positives}, TN: {true_negatives}, FP: {false_positives}, FN: {false_negatives}")
    print(f"  Accuracy: {search_mode_results.accuracy:.3f}, Precision: {search_mode_results.precision:.3f}, Recall: {search_mode_results.recall:.3f}")
    
    return search_mode_results


async def run_searches(session: aiohttp.ClientSession, index_name: str, config: BenchmarkConfig, fp_generator: FingerprintGenerator) -> dict[str, Any]:
    """Run searches across three different modes."""
    url = f'http://localhost:{config.server_port}/{index_name}/_search'
    
    # Run three different search modes
    print("\nRunning search benchmark across three modes...")
    print(f"DEBUG: FingerprintGenerator has {len(fp_generator.inserted_ids)} inserted fingerprints tracked")
    if len(fp_generator.inserted_ids) <= 5:
        print(f"DEBUG: Inserted IDs: {fp_generator.inserted_ids}")
        for fp_id in fp_generator.inserted_ids[:3]:  # Show first 3
            hashes = fp_generator.inserted_hashes[fp_id]
            print(f"DEBUG: FP {fp_id} has {len(hashes)} hashes: {hashes[:10]}...")
    
    # Mode 1: Mostly matches (90% should find matches)
    mostly_matches_results = await run_search_mode(
        session, url, "mostly matches", config.searches_per_mode, 
        config.match_probability_high, fp_generator, config
    )
    
    # Mode 2: Mostly misses (10% should find matches) 
    mostly_misses_results = await run_search_mode(
        session, url, "mostly misses", config.searches_per_mode,
        config.match_probability_low, fp_generator, config
    )
    
    # Mode 3: Mixed (50% should find matches)
    mixed_results = await run_search_mode(
        session, url, "mixed", config.searches_per_mode,
        config.match_probability_mixed, fp_generator, config
    )
    
    total_searches = config.searches_per_mode * 3
    total_matches = mostly_matches_results.matches_found + mostly_misses_results.matches_found + mixed_results.matches_found
    total_expected = mostly_matches_results.expected_matches + mostly_misses_results.expected_matches + mixed_results.expected_matches
    
    overall_avg = mean([mostly_matches_results.avg_latency, mostly_misses_results.avg_latency, mixed_results.avg_latency])
    overall_p95 = mean([mostly_matches_results.p95_latency, mostly_misses_results.p95_latency, mixed_results.p95_latency])
    
    # Calculate overall accuracy metrics
    total_tp = mostly_matches_results.true_positives + mostly_misses_results.true_positives + mixed_results.true_positives
    total_tn = mostly_matches_results.true_negatives + mostly_misses_results.true_negatives + mixed_results.true_negatives
    total_fp = mostly_matches_results.false_positives + mostly_misses_results.false_positives + mixed_results.false_positives
    total_fn = mostly_matches_results.false_negatives + mostly_misses_results.false_negatives + mixed_results.false_negatives
    
    overall_accuracy = (total_tp + total_tn) / total_searches
    overall_precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    overall_recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    
    print("\n=== OVERALL SEARCH SUMMARY ===")
    print(f"Expected matches: {total_expected}/{total_searches} ({total_expected/total_searches:.1%})")
    print(f"Actual matches: {total_matches}/{total_searches} ({total_matches/total_searches:.1%})")
    print(f"Overall accuracy: {overall_accuracy:.3f}, precision: {overall_precision:.3f}, recall: {overall_recall:.3f}")
    print(f"Overall avg latency: {overall_avg*1000:.2f}ms, overall p95 latency: {overall_p95*1000:.2f}ms")
    
    return {
        'mostly_matches_avg_latency': mostly_matches_results.avg_latency,
        'mostly_matches_p95_latency': mostly_matches_results.p95_latency,
        'mostly_matches_found': mostly_matches_results.matches_found,
        'mostly_matches_expected': mostly_matches_results.expected_matches,
        'mostly_matches_accuracy': mostly_matches_results.accuracy,
        'mostly_matches_precision': mostly_matches_results.precision,
        'mostly_matches_recall': mostly_matches_results.recall,
        'mostly_matches_tp': mostly_matches_results.true_positives,
        'mostly_matches_tn': mostly_matches_results.true_negatives,
        'mostly_matches_fp': mostly_matches_results.false_positives,
        'mostly_matches_fn': mostly_matches_results.false_negatives,
        
        'mostly_misses_avg_latency': mostly_misses_results.avg_latency,
        'mostly_misses_p95_latency': mostly_misses_results.p95_latency,
        'mostly_misses_found': mostly_misses_results.matches_found,
        'mostly_misses_expected': mostly_misses_results.expected_matches,
        'mostly_misses_accuracy': mostly_misses_results.accuracy,
        'mostly_misses_precision': mostly_misses_results.precision,
        'mostly_misses_recall': mostly_misses_results.recall,
        'mostly_misses_tp': mostly_misses_results.true_positives,
        'mostly_misses_tn': mostly_misses_results.true_negatives,
        'mostly_misses_fp': mostly_misses_results.false_positives,
        'mostly_misses_fn': mostly_misses_results.false_negatives,
        
        'mixed_avg_latency': mixed_results.avg_latency,
        'mixed_p95_latency': mixed_results.p95_latency,
        'mixed_found': mixed_results.matches_found,
        'mixed_expected': mixed_results.expected_matches,
        'mixed_accuracy': mixed_results.accuracy,
        'mixed_precision': mixed_results.precision,
        'mixed_recall': mixed_results.recall,
        'mixed_tp': mixed_results.true_positives,
        'mixed_tn': mixed_results.true_negatives,
        'mixed_fp': mixed_results.false_positives,
        'mixed_fn': mixed_results.false_negatives,
        
        'overall_avg_latency': overall_avg,
        'overall_p95_latency': overall_p95,
        'total_searches': total_searches,
        'total_matches': total_matches,
        'total_expected': total_expected,
        'overall_accuracy': overall_accuracy,
        'overall_precision': overall_precision,
        'overall_recall': overall_recall,
        'total_tp': total_tp,
        'total_tn': total_tn,
        'total_fp': total_fp,
        'total_fn': total_fn,
    }


async def single_benchmark_run(run_number: int, config: BenchmarkConfig) -> dict[str, float]:
    data_dir = Path(config.data_dir_base) / f"run_{run_number}"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Create fingerprint generator for this run
    fp_generator = FingerprintGenerator(config)
    
    print(f"Hash space: {config.get_hash_space_info()}")
    print(f"Random seed: {config.random_seed}")

    async with ServerManager(str(data_dir), config.server_port, config) as server:
        async with aiohttp.ClientSession() as session:
            await create_index(session, config.index_name, config.server_port)

            insertion_results = await run_insertion(session, config.index_name, config.num_docs, config, fp_generator)

            # Allow some time for the server to merge segments
            await asyncio.sleep(config.merge_wait_time)

            search_results = await run_searches(session, config.index_name, config, fp_generator)

            data_size = server.get_data_size()
            print(f"Data size: {data_size / (1024*1024):.2f} MB")

    return {
        **insertion_results,
        **search_results,
        'data_size': data_size,
    }


def report_insertion_metrics(results: list[dict[str, Any]]) -> None:
    """Report insertion performance metrics."""
    # Extract metrics for each insert mode
    bulk_rates = [r['bulk_insertion_rate'] for r in results]
    medium_rates = [r['medium_insertion_rate'] for r in results]
    small_rates = [r['small_insertion_rate'] for r in results]
    overall_rates = [r['overall_insertion_rate'] for r in results]

    print("\n=== INSERTION PERFORMANCE ===")
    print("Bulk Load Insertion Rate (docs/s):")
    print(f"  Avg: {mean(bulk_rates):.2f}")
    print(f"  Std: {std(bulk_rates):.2f}")

    print("Medium Batch Insertion Rate (docs/s):")
    print(f"  Avg: {mean(medium_rates):.2f}")
    print(f"  Std: {std(medium_rates):.2f}")

    print("Small Batch Insertion Rate (docs/s):")
    print(f"  Avg: {mean(small_rates):.2f}")
    print(f"  Std: {std(small_rates):.2f}")

    print("Overall Insertion Rate (docs/s):")
    print(f"  Avg: {mean(overall_rates):.2f}")
    print(f"  Std: {std(overall_rates):.2f}")


def report_search_performance(results: list[dict[str, Any]]) -> None:
    """Report search performance metrics."""
    # Extract search metrics for each mode
    mostly_matches_avg = [r['mostly_matches_avg_latency'] * 1000 for r in results]
    mostly_matches_p95 = [r['mostly_matches_p95_latency'] * 1000 for r in results]
    mostly_misses_avg = [r['mostly_misses_avg_latency'] * 1000 for r in results]
    mostly_misses_p95 = [r['mostly_misses_p95_latency'] * 1000 for r in results]
    mixed_avg = [r['mixed_avg_latency'] * 1000 for r in results]
    mixed_p95 = [r['mixed_p95_latency'] * 1000 for r in results]
    overall_search_avg = [r['overall_avg_latency'] * 1000 for r in results]
    overall_search_p95 = [r['overall_p95_latency'] * 1000 for r in results]
    data_sizes = [r['data_size'] / (1024*1024) for r in results]

    print("\n=== SEARCH PERFORMANCE BY MODE ===")
    print("Mostly Matches - Average Latency (ms):")
    print(f"  Avg: {mean(mostly_matches_avg):.2f}")
    print(f"  Std: {std(mostly_matches_avg):.2f}")
    
    print("Mostly Matches - P95 Latency (ms):")
    print(f"  Avg: {mean(mostly_matches_p95):.2f}")
    print(f"  Std: {std(mostly_matches_p95):.2f}")
    
    print("Mostly Misses - Average Latency (ms):")
    print(f"  Avg: {mean(mostly_misses_avg):.2f}")
    print(f"  Std: {std(mostly_misses_avg):.2f}")
    
    print("Mostly Misses - P95 Latency (ms):")
    print(f"  Avg: {mean(mostly_misses_p95):.2f}")
    print(f"  Std: {std(mostly_misses_p95):.2f}")
    
    print("Mixed - Average Latency (ms):")
    print(f"  Avg: {mean(mixed_avg):.2f}")
    print(f"  Std: {std(mixed_avg):.2f}")
    
    print("Mixed - P95 Latency (ms):")
    print(f"  Avg: {mean(mixed_p95):.2f}")
    print(f"  Std: {std(mixed_p95):.2f}")

    print("\n=== OVERALL SEARCH PERFORMANCE ===")
    print("Overall Average Latency (ms):")
    print(f"  Avg: {mean(overall_search_avg):.2f}")
    print(f"  Std: {std(overall_search_avg):.2f}")
    
    print("Overall P95 Latency (ms):")
    print(f"  Avg: {mean(overall_search_p95):.2f}")
    print(f"  Std: {std(overall_search_p95):.2f}")

    print("\n=== STORAGE ===")
    print("Data Size (MB):")
    print(f"  Avg: {mean(data_sizes):.2f}")
    print(f"  Std: {std(data_sizes):.2f}")


def report_accuracy_metrics(results: list[dict[str, Any]]) -> None:
    """Report search accuracy metrics and distribution analysis."""
    # Accuracy metrics for each mode
    mostly_matches_accuracy = [r['mostly_matches_accuracy'] for r in results]
    mostly_misses_accuracy = [r['mostly_misses_accuracy'] for r in results]
    mixed_accuracy = [r['mixed_accuracy'] for r in results]
    overall_accuracy = [r['overall_accuracy'] for r in results]
    
    print("\n=== SEARCH ACCURACY METRICS ===")
    print("Mostly Matches Mode Accuracy:")
    print(f"  Avg: {mean(mostly_matches_accuracy):.3f}")
    print(f"  Std: {std(mostly_matches_accuracy):.3f}")
    
    print("Mostly Misses Mode Accuracy:")
    print(f"  Avg: {mean(mostly_misses_accuracy):.3f}")
    print(f"  Std: {std(mostly_misses_accuracy):.3f}")
    
    print("Mixed Mode Accuracy:")
    print(f"  Avg: {mean(mixed_accuracy):.3f}")
    print(f"  Std: {std(mixed_accuracy):.3f}")
    
    print("Overall Accuracy:")
    print(f"  Avg: {mean(overall_accuracy):.3f}")
    print(f"  Std: {std(overall_accuracy):.3f}")
    
    print("\n=== DOCUMENT & SEARCH DISTRIBUTION ===")
    if results:
        sample_result = results[0]
        print(f"Bulk load docs: {sample_result['bulk_docs_count']}")
        print(f"Medium batch docs: {sample_result['medium_docs_count']}")
        print(f"Small batch docs: {sample_result['small_docs_count']}")
        total_docs = sum([sample_result['bulk_docs_count'], sample_result['medium_docs_count'], sample_result['small_docs_count']])
        print(f"Total docs: {total_docs}")
        print()
        print(f"Searches per mode: {sample_result['total_searches']//3}")
        print(f"Total searches: {sample_result['total_searches']}")
        
        # Show expected vs actual match rates
        mostly_matches_expected_rate = mean([r['mostly_matches_expected'] for r in results]) / (sample_result['total_searches']//3)
        mostly_matches_actual_rate = mean([r['mostly_matches_found'] for r in results]) / (sample_result['total_searches']//3)
        
        mostly_misses_expected_rate = mean([r['mostly_misses_expected'] for r in results]) / (sample_result['total_searches']//3)
        mostly_misses_actual_rate = mean([r['mostly_misses_found'] for r in results]) / (sample_result['total_searches']//3)
        
        mixed_expected_rate = mean([r['mixed_expected'] for r in results]) / (sample_result['total_searches']//3)
        mixed_actual_rate = mean([r['mixed_found'] for r in results]) / (sample_result['total_searches']//3)
        
        overall_expected_rate = mean([r['total_expected'] for r in results]) / sample_result['total_searches']
        overall_actual_rate = mean([r['total_matches'] for r in results]) / sample_result['total_searches']
        
        print()
        print("Expected vs Actual Match Rates:")
        print(f"  Mostly matches mode: {mostly_matches_expected_rate:.1%} expected, {mostly_matches_actual_rate:.1%} actual")
        print(f"  Mostly misses mode: {mostly_misses_expected_rate:.1%} expected, {mostly_misses_actual_rate:.1%} actual")
        print(f"  Mixed mode: {mixed_expected_rate:.1%} expected, {mixed_actual_rate:.1%} actual")
        print(f"  Overall: {overall_expected_rate:.1%} expected, {overall_actual_rate:.1%} actual")
        
        print()
        print("False Positive/Negative Analysis:")
        total_tp = mean([r['total_tp'] for r in results])
        total_tn = mean([r['total_tn'] for r in results])
        total_fp = mean([r['total_fp'] for r in results])
        total_fn = mean([r['total_fn'] for r in results])
        
        print(f"  True Positives (expected match, got match): {total_tp:.1f}")
        print(f"  True Negatives (expected miss, got miss): {total_tn:.1f}")
        print(f"  False Positives (expected miss, got match): {total_fp:.1f}")
        print(f"  False Negatives (expected match, got miss): {total_fn:.1f}")
        
        if total_fp > 0:
            print(f"  ⚠️  Algorithm found unexpected matches ({total_fp:.1f} false positives)")
        if total_fn > 0:
            print(f"  ⚠️  Algorithm missed expected matches ({total_fn:.1f} false negatives)")
        if total_tp == 0 and total_fp == 0:
            print("  ℹ️   No matches found at all - algorithm may be very strict or incompatible data format")


async def run_benchmark(config: BenchmarkConfig) -> None:
    """Run the complete benchmark and report results."""
    results = []
    for i in range(1, config.num_runs + 1):
        print(f"--- Starting benchmark run {i}/{config.num_runs} ---")
        result = await single_benchmark_run(i, config)
        results.append(result)

    print("\n--- Benchmark Summary ---")
    
    report_insertion_metrics(results)
    report_search_performance(results)
    report_accuracy_metrics(results)


def create_config_from_args() -> BenchmarkConfig:
    config = BenchmarkConfig()
    parser = argparse.ArgumentParser(description="Run the fpindex benchmark.")
    parser.add_argument('--num-docs', type=int, default=config.num_docs, 
                       help=f'Number of documents to insert (default: {config.num_docs})')
    parser.add_argument('--num-searches', type=int, default=config.num_searches, 
                       help=f'Number of searches to perform (default: {config.num_searches})')
    parser.add_argument('--num-runs', type=int, default=config.num_runs, 
                       help=f'Number of benchmark runs (default: {config.num_runs})')
    parser.add_argument('--port', type=int, default=config.server_port, 
                       help=f'Server port (default: {config.server_port})')
    parser.add_argument('--bulk-batch-size', type=int, default=config.bulk_batch_size, 
                       help=f'Bulk mode batch size (default: {config.bulk_batch_size})')
    parser.add_argument('--medium-batch-min', type=int, default=config.medium_batch_min, 
                       help=f'Medium mode min batch size (default: {config.medium_batch_min})')
    parser.add_argument('--medium-batch-max', type=int, default=config.medium_batch_max, 
                       help=f'Medium mode max batch size (default: {config.medium_batch_max})')
    parser.add_argument('--small-batch-min', type=int, default=config.small_batch_min, 
                       help=f'Small mode min batch size (default: {config.small_batch_min})')
    parser.add_argument('--small-batch-max', type=int, default=config.small_batch_max, 
                       help=f'Small mode max batch size (default: {config.small_batch_max})')
    parser.add_argument('--searches-per-mode', type=int, default=config.searches_per_mode, 
                       help=f'Number of searches per mode (default: {config.searches_per_mode})')
    parser.add_argument('--random-seed', type=int, default=config.random_seed, 
                       help=f'Random seed for reproducible results (default: {config.random_seed})')
    parser.add_argument('--hash-bits', type=int, default=config.hash_bits, 
                       help=f'Number of bits for hash values (default: {config.hash_bits}, range: 8-32)')
    parser.add_argument('--server-exe', type=str, default=config.server_exe, 
                       help=f'Path to server executable (default: {config.server_exe})')
    args = parser.parse_args()
    
    config.num_docs = args.num_docs
    config.num_searches = args.num_searches
    config.num_runs = args.num_runs
    config.server_port = args.port
    config.bulk_batch_size = args.bulk_batch_size
    config.medium_batch_min = args.medium_batch_min
    config.medium_batch_max = args.medium_batch_max
    config.small_batch_min = args.small_batch_min
    config.small_batch_max = args.small_batch_max
    config.searches_per_mode = args.searches_per_mode
    config.random_seed = args.random_seed
    config.hash_bits = max(8, min(32, args.hash_bits))  # Clamp to reasonable range
    config.server_exe = args.server_exe
    
    return config


def main() -> None:
    config = create_config_from_args()
    
    if not os.path.exists(config.server_exe):
        print(f"Error: {config.server_exe} not found. Please ensure the project is built with 'zig build --release=fast'.")
        raise SystemExit(1)
    
    try:
        asyncio.run(run_benchmark(config))
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
        raise SystemExit(1)
    except Exception as e:
        print(f"Benchmark failed with error: {e}")
        raise SystemExit(1)


if __name__ == '__main__':
    main()
